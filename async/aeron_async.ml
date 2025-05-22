open Core
open Async
open Aeron

let src = Logs.Src.create "aeron.async"

module Lo = (val Logs.src_log src : Logs.LOG)

module MkPublication (P : Publication_sig) = struct
  let add t uri streamID =
    let wait = P.add t uri streamID in
    let rec loop () =
      match P.add_poll wait with
      | None -> Scheduler.yield () >>= loop
      | Some x -> return x
    in
    loop ()
  ;;

  let close t =
    P.close t;
    let rec loop () =
      match P.is_closed t with
      | true -> Deferred.unit
      | false -> Scheduler.yield () >>= loop
    in
    loop ()
  ;;

  let offer t msg =
    let sub = Iobuf.Consume.To_bigstring.subo msg in
    P.offer t sub
  ;;

  let consts = P.consts
end

module ConcurrentPublication = MkPublication (Publication)
module ExclusivePublication = MkPublication (ExclusivePublication)

type 'a publication =
  { pub : pub_kind
  ; encode : 'a -> (read, Iobuf.seek) Iobuf.t
  }

and pub_kind =
  | Concurrent of Aeron.Publication.t
  | Exclusive of Aeron.ExclusivePublication.t

let add_concurrent t uri streamID encode =
  ConcurrentPublication.add t uri streamID >>| fun x -> { pub = Concurrent x; encode }
;;

let add_exclusive t uri streamID encode =
  ExclusivePublication.add t uri streamID >>| fun x -> { pub = Exclusive x; encode }
;;

let publication_consts { pub; _ } =
  match pub with
  | Concurrent x -> ConcurrentPublication.consts x
  | Exclusive x -> ExclusivePublication.consts x
;;

type subscription =
  { sub : Subscription.t
  ; r : Reader.t
  }
[@@deriving fields]

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

and pub = P : 'a publication -> pub [@@deriving fields]

let offer t { pub; encode } msg =
  if Ivar.is_full t.stop then invalid_arg "offer: context closed";
  match pub with
  | Concurrent pub -> ConcurrentPublication.offer pub (encode msg)
  | Exclusive pub -> ExclusivePublication.offer pub (encode msg)
;;

let close_publication t { pub; _ } =
  if Ivar.is_full t.stop then invalid_arg "close_publication: context closed";
  match pub with
  | Concurrent x -> ConcurrentPublication.close x
  | Exclusive x -> ExclusivePublication.close x
;;

let close ({ client; ctx; pubs; stop; subs; _ } as t) =
  match Ivar.is_full stop with
  | true ->
    (* Already closed! Not idempotent! *)
    Deferred.unit
  | false ->
    Lo.debug (fun m -> m "start closing aeron client");
    let pubs = Hashtbl.to_alist pubs in
    let subs = Hashtbl.to_alist subs in
    Deferred.List.iter subs ~how:`Parallel ~f:(fun (_, { r; _ }) -> Reader.close r)
    >>= fun () ->
    Deferred.List.iter pubs ~how:`Parallel ~f:(fun (_, P pub) -> close_publication t pub)
    >>| fun () ->
    (* stop polling *)
    Lo.debug (fun m -> m "closing aeron client: fill ivar and call close");
    Aeron.close client;
    Aeron.Context.close ctx;
    (* Signaling the start of closing? This will trigger a reconnect
     when using persistent connection. *)
    Ivar.fill_if_empty stop ()
;;

let start_polling t on_error span =
  let stop = Ivar.read t.stop in
  Clock_ns.run_at_intervals ~stop ~continue_on_error:true span (fun () ->
    match main_do_work t.client with
    | -1 ->
      (* Synchronous errors *)
      on_error (errcode ()) (errmsg ());
      (* TODO: close only if its fatal. Are all errors fatal? *)
      don't_wait_for (close t)
    | _ -> ())
;;

let process_reader t on_error r =
  let hdr = Bigstring.create 8 in
  let hdrs = Bigsubstring.create hdr in
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
       | `Eof _ -> Deferred.unit
       | `Ok ->
         on_error errcode (Substring.to_string strs);
         (* TODO: close only if fatal. *)
         don't_wait_for (close t);
         loop ())
  in
  loop ()
;;

(* We set a timeout of one second by default. *)
let create ?driver_timeout ?(idle = Time_ns.Span.of_int_ms 1) ~on_error dir =
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
  don't_wait_for (process_reader t on_error (Reader.create r));
  start_polling t on_error idle;
  t
;;

let is_closed { stop; _ } = Ivar.is_full stop
let close_finished { stop; _ } = Ivar.read stop

let add_concurrent_publication { client; pubs; stop; _ } chan ~streamID encode =
  if Ivar.is_full stop then invalid_arg "add_concurrent_publication: context closed";
  add_concurrent client chan streamID encode
  >>| fun x ->
  let consts = publication_consts x in
  Hashtbl.set pubs ~key:consts.registration_id ~data:(P x);
  x, consts
;;

let add_exclusive_publication { client; pubs; stop; _ } chan ~streamID encode =
  if Ivar.is_full stop then invalid_arg "add_exclusive_publication: context closed";
  add_exclusive client chan streamID encode
  >>| fun x ->
  let consts = publication_consts x in
  Hashtbl.set pubs ~key:consts.registration_id ~data:(P x);
  x, consts
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
  if Ivar.is_full t.stop
  then invalid_arg "close_subscription: context closed"
  else close_subscription_aux x
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
  if Ivar.is_full t.stop then invalid_arg "add_subscription: context closed";
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
