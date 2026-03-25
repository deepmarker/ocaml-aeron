open Core
open Async
open Aeron

let src = Logs.Src.create "aeron.async"

module Lo = (val Logs.src_log src : Logs.LOG)

module Encoder = struct
  type ('a, _) t =
    | Alloc : ('a -> Bigstring.t) -> ('a, [ `Alloc ]) t
    | Direct :
        { claim : claim
        ; sizer : 'a -> int
        ; f : Bigstring.t -> 'a -> unit
        }
        -> ('a, [ `Direct ]) t

  let alloc a = Alloc a
  let direct sizer f = Direct { sizer; f; claim = alloc_claim () }
end

module type S = sig
  type t

  val add : Aeron.t -> Uri.t -> int32 -> t Deferred.t
  val is_closed : t -> bool
  val close_finished : t -> unit Deferred.t
  val close : t -> unit Deferred.t
  val offer : t -> ?pos:int -> ?len:int -> Bigstringaf.t -> (int, OfferError.t) result
  val tryclaim : t -> int -> claim -> (int, OfferError.t) result
  val consts : t -> pub_consts
end

module MkAsyncPublication (P : Publication_sig) : S = struct
  type t =
    { pub : P.t
    ; closed : unit Ivar.t
    }

  let add t uri streamID =
    let wait = P.add t uri streamID in
    let rec loop () =
      match P.add_poll wait with
      | None -> Scheduler.yield () >>= loop
      | Some x -> return { pub = x; closed = Ivar.create () }
    in
    loop ()
  ;;

  let is_closed { pub; _ } = P.is_closed pub
  let close_finished { closed; _ } = Ivar.read closed

  (* idempotent. *)
  let close { pub; closed } =
    match Ivar.is_full closed with
    | true -> Deferred.unit
    | false ->
      P.close pub;
      let rec loop () =
        match P.is_closed pub with
        | true ->
          Ivar.fill_if_empty closed ();
          Deferred.unit
        | false -> Scheduler.yield () >>= loop
      in
      loop ()
  ;;

  let offer { pub; closed } ?pos ?len buf =
    let res = P.offer ?pos ?len pub buf in
    match res with
    | Error Closed ->
      Ivar.fill_if_empty closed ();
      res
    | _ -> res
  ;;

  let tryclaim { pub; closed } i claim =
    let res = P.tryclaim pub i claim in
    match res with
    | Error Closed ->
      Ivar.fill_if_empty closed ();
      res
    | _ -> res
  ;;

  let consts { pub; _ } = P.consts pub
end

module MkPublication (S : S) = struct
  module PPub = Persistent_connection_kernel.Make (S)

  type ('a, 'b) t =
    { pub : PPub.t
    ; encode : ('a, 'b) Encoder.t
    }

  module Address = struct
    type t =
      { chan : Uri_sexp.t
      ; stream_id : int32
      }
    [@@deriving sexp, compare, equal]
  end

  (* Add a connection. *)
  let create t chan stream_id encode =
    let pub =
      PPub.create
        ~server_name:""
        ~address:(module Address)
        ~connect:(fun { Address.chan; stream_id } ->
          Monitor.try_with_or_error (fun () -> S.add t chan stream_id))
        (fun () -> Deferred.Or_error.return { Address.chan; stream_id })
    in
    { pub; encode }
  ;;

  let consts { pub; _ } = PPub.connected_or_failed_to_connect pub >>|? S.consts

  let offer { pub; _ } ?pos ?len s =
    PPub.connected_or_failed_to_connect pub >>|? fun x -> S.offer x ?pos ?len s
  ;;

  let tryclaim { pub; _ } i claim =
    PPub.connected_or_failed_to_connect pub >>|? fun x -> S.tryclaim x i claim
  ;;

  let handle_direct pub sizer f claim msg =
    let len = sizer msg in
    tryclaim pub len claim
    >>|? function
    | Error err -> Result.fail err
    | Ok newpos ->
      let bs = bigstring_of_claim claim in
      f bs msg;
      if commit_claim claim <> 0 then failwith "commit claim failed";
      Result.return newpos
  ;;
end

module Concurrent = MkPublication (MkAsyncPublication (Publication))
module Exclusive = MkPublication (MkAsyncPublication (ExclusivePublication))

type subscription =
  { sub : Subscription.t
  ; r : Reader.t
  }
[@@deriving fields]

exception Stopped

type ('a, 'b) publication =
  | Concurrent of ('a, 'b) Concurrent.t
  | Exclusive of ('a, 'b) Exclusive.t

let close_publication = function
  | Concurrent { pub; _ } -> Concurrent.PPub.close pub
  | Exclusive { pub; _ } -> Exclusive.PPub.close pub
;;

type t =
  { ctx : Context.t
  ; ba : Bigstring.t
    (* useless here but must not be GCed, contains fd information for
       C -> OCaml errors. *)
  ; client : Aeron.t
  ; pubs : pub Int64.Table.t
  ; stop : unit Ivar.t
  ; subs : subscription Int64.Table.t
  }

and pub = P : ('a, 'b) publication -> pub [@@deriving fields]

let offer
  : type a b.
    t -> (a, b) publication -> a -> (int, OfferError.t) result Deferred.Or_error.t
  =
  fun t pub msg ->
  if Ivar.is_full t.stop then raise Stopped;
  match pub with
  | Concurrent ({ encode = Alloc f; _ } as pub) -> Concurrent.offer pub (f msg)
  | Concurrent ({ encode = Direct { sizer; f; claim }; _ } as pub) ->
    Concurrent.handle_direct pub sizer f claim msg
  | Exclusive ({ encode = Alloc f; _ } as pub) -> Exclusive.offer pub (f msg)
  | Exclusive ({ encode = Direct { sizer; f; claim }; _ } as pub) ->
    Exclusive.handle_direct pub sizer f claim msg
;;

let close { client; ctx; pubs; stop; subs; _ } =
  match Ivar.is_full stop with
  | true ->
    (* Already closed! Not idempotent! *)
    Deferred.unit
  | false ->
    (* Signaling the start of closing? This will trigger a reconnect
     when using persistent connection. *)
    Ivar.fill_if_empty stop ();
    Lo.debug (fun m -> m "start closing aeron client");
    let pubs = Hashtbl.to_alist pubs in
    let subs = Hashtbl.to_alist subs in
    Monitor.protect
      (fun () ->
         Deferred.List.iter subs ~how:`Parallel ~f:(fun (_, { r; _ }) -> Reader.close r)
         >>= fun () ->
         Deferred.List.iter pubs ~how:`Parallel ~f:(fun (_, P x) ->
           match x with
           | Concurrent { pub; _ } -> Concurrent.PPub.close pub
           | Exclusive { pub; _ } -> Exclusive.PPub.close pub))
      ~finally:(fun () ->
        (* stop polling *)
        Lo.debug (fun m -> m "closing (freeing) aeron client C structures");
        Aeron.close client;
        Aeron.Context.close ctx;
        Deferred.unit)
;;

exception
  WorkError of
    { err : Err.t
    ; msg : string
    }

let do_work_exn t =
  match main_do_work t.client with
  | -1 -> raise (WorkError { err = errcode (); msg = errmsg () })
  | _ -> ()
;;

let get_error_pipe r =
  let hdr = Bigstring.create 8 in
  let hdrs = Bigsubstring.create hdr in
  let on_w w =
    let rec loop () =
      Reader.really_read_bigsubstring r hdrs
      >>= function
      | `Eof _ -> Deferred.unit
      | `Ok ->
        let errcode = Bigstring.get_int32_be hdr ~pos:0 |> Err.of_int in
        let len = Bigstring.get_int32_be hdr ~pos:4 in
        let bytes = Bytes.create len in
        let strs = Substring.create bytes in
        Reader.really_read_substring r strs
        >>= (function
         | `Eof _ ->
           Pipe.close w;
           Deferred.unit
         | `Ok ->
           let msg = Substring.to_string strs in
           let err =
             Error.create_s
               [%message "AeronError" ~err:(errcode : Err.t) ~msg:(msg : string)]
           in
           Pipe.write_if_open w err >>= loop)
    in
    loop ()
  in
  Pipe.create_reader ~close_on_exception:false on_w
;;

(* We set a timeout of one second by default. *)
let create ?driver_timeout dir =
  let nfo = Info.create_s [%message "Aeron_async.create"] in
  Unix.pipe nfo
  >>= fun (`Reader r, `Writer w) ->
  let ba = Bigstring.create 8 in
  Bigstring.set_uint16_be_exn ba ~pos:0 (Fd.to_int_exn w);
  let ctx = Context.create ba in
  Context.set_dir ctx dir;
  Option.iter driver_timeout ~f:(fun x ->
    Context.set_driver_timeout_ms ctx (Time_ns.Span.to_int_ms x));
  Context.set_use_conductor_agent_invoker ctx true;
  (* this might block then return an exn. *)
  Monitor.try_with_or_error (fun () -> In_thread.run (fun () -> init_exn ctx))
  >>|? fun client ->
  let pubs = Int64.Table.create () in
  let stop = Ivar.create () in
  let subs = Int64.Table.create () in
  let t = Fields.create ~ctx ~ba ~client ~pubs ~stop ~subs in
  t, get_error_pipe (Reader.create r)
;;

let is_closed { stop; _ } = Ivar.is_full stop
let close_finished { stop; _ } = Ivar.read stop

let add_concurrent_publication { client; pubs; stop; _ } chan ~streamID encode =
  if Ivar.is_full stop then raise Stopped;
  let x = Concurrent.create client chan streamID encode in
  Concurrent.consts x
  >>|? fun consts ->
  Hashtbl.set pubs ~key:consts.registration_id ~data:(P (Concurrent x));
  Concurrent x, consts
;;

let add_exclusive_publication { client; pubs; stop; _ } chan ~streamID encode =
  if Ivar.is_full stop then raise Stopped;
  let x = Exclusive.create client chan streamID encode in
  Exclusive.consts x
  >>|? fun consts ->
  Hashtbl.set pubs ~key:consts.registration_id ~data:(P (Exclusive x));
  Exclusive x, consts
;;

let close_subscription_aux { sub; r } =
  Subscription.close sub;
  let rec loop () =
    match Subscription.is_closed sub with
    | true ->
      (* cleanup *)
      Reader.close r
    | false -> Scheduler.yield () >>= loop
  in
  loop ()
;;

let close_subscription t x =
  if Ivar.is_full t.stop then raise Stopped else close_subscription_aux x
;;

let start_polling_subscription
      ?(stop = Deferred.never ())
      ?(period = Time_ns.Span.of_int_ms 1)
      ?(max_fragments = 10)
      (sub : subscription)
      f
  =
  (* Repeatedly [max_fragments] till [stop] is determined. *)
  let close_sub =
    lazy
      (Lo.debug (fun m -> m "Closing subscription");
       close_subscription_aux sub
       >>| fun () -> Lo.debug (fun m -> m "Closed subscription"))
  in
  (* Launch polling loop. *)
  don't_wait_for
    (let rec loop () =
       if Reader.is_closed sub.r
       then Deferred.unit
       else (
         match Aeron.Subscription.poll_exn sub.sub max_fragments with
         | exception exn ->
           Lo.err (fun m -> m "%s" (Exn.to_string exn));
           Lazy.force close_sub
         | _nb_frags when Deferred.is_determined stop -> Lazy.force close_sub
         | _ -> Clock_ns.after period >>= loop)
     in
     loop ());
  let bbuf = Bigbuffer.create 4096 in
  let hdr = Bigstring.create Header.sizeof_values in
  let shdr = Bigsubstring.create hdr in
  let buf = Bigstring.create 4096 in
  (* Read data from the C callback via a fd/Reader.t *)
  let rec loop () =
    (* read hdr *)
    Reader.really_read_bigsubstring sub.r shdr
    >>= function
    | `Eof _ ->
      (* TODO: ok? *)
      Deferred.unit
    | `Ok ->
      let h = Header.of_cstruct (Cstruct.of_bigarray hdr) in
      let len = Int32.to_int_exn h.frame.frame_length - 32 in
      (* now read len bytes of payload *)
      Lo.debug (fun m -> m "Read %d bytes from sub" len);
      Reader.really_read_bigsubstring sub.r (Bigsubstring.create buf ~len)
      >>= (function
       | `Eof _ ->
         (* TODO: ok? *)
         Deferred.unit
       | `Ok ->
         (* Lo.debug (fun m -> m "%a" Cstruct.hexdump_pp (Cstruct.of_bigarray buf ~len)); *)
         (match h.frame.flags lsr 6 with
          | 3 ->
            (* unique frame *)
            f (Iobuf.of_bigstring buf ~len);
            loop ()
          | 2 ->
            (* first frame *)
            Bigbuffer.clear bbuf;
            Bigbuffer.add_bigstring bbuf (Bigstring.sub_shared buf ~len);
            loop ()
          | 0 ->
            Bigbuffer.add_bigstring bbuf (Bigstring.sub_shared buf ~len);
            loop ()
          | _ ->
            (* last frame *)
            Bigbuffer.add_bigstring bbuf (Bigstring.sub_shared buf ~len);
            let len = Bigbuffer.length bbuf in
            f (Iobuf.of_bigstring (Bigbuffer.volatile_contents bbuf) ~len);
            loop ()))
  in
  loop ()
;;

let add_subscription ?stop ?period ?max_fragments t uri ~streamID f =
  if Ivar.is_full t.stop then raise Stopped;
  Unix.pipe (Info.of_string "Aeron_async.add_subscription")
  >>= function
  | `Reader rfd, `Writer wfd ->
    let sub_req = Aeron.Subscription.add t.client uri streamID in
    let wfd_raw = Fd.to_int_exn wfd in
    let rec loop () =
      match Aeron.Subscription.add_poll sub_req wfd_raw with
      | None -> Scheduler.yield () >>= loop
      | Some sub ->
        let consts = Subscription.consts sub in
        let r = Reader.create rfd in
        let sub = Fields_of_subscription.create ~sub ~r in
        Hashtbl.set t.subs ~key:consts.registration_id ~data:sub;
        don't_wait_for (start_polling_subscription ?stop ?period ?max_fragments sub f);
        return (sub, consts)
    in
    loop ()
;;
