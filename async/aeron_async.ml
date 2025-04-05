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

module Publication = struct
  module ConcurrentPublication = MkPublication (Publication)
  module ExclusivePublication = MkPublication (ExclusivePublication)

  type pub =
    | Concurrent of Aeron.Publication.t
    | Exclusive of Aeron.ExclusivePublication.t

  type 'a t =
    { pub : pub
    ; encode : 'a -> (read, Iobuf.seek) Iobuf.t
    }

  let close { pub; _ } =
    match pub with
    | Concurrent x -> ConcurrentPublication.close x
    | Exclusive x -> ExclusivePublication.close x
  ;;

  let add_concurrent t uri streamID encode =
    ConcurrentPublication.add t uri streamID >>| fun x -> { pub = Concurrent x; encode }
  ;;

  let add_exclusive t uri streamID encode =
    ExclusivePublication.add t uri streamID >>| fun x -> { pub = Exclusive x; encode }
  ;;

  let offer { pub; encode } msg =
    match pub with
    | Concurrent x -> ConcurrentPublication.offer x (encode msg)
    | Exclusive x -> ExclusivePublication.offer x (encode msg)
  ;;

  let consts { pub; _ } =
    match pub with
    | Concurrent x -> ConcurrentPublication.consts x
    | Exclusive x -> ExclusivePublication.consts x
  ;;
end

module Endpoint = struct
  module T = struct
    type t =
      { chn : string
      ; stream : int32
      }
    [@@deriving compare, hash, bin_io, sexp, fields]
  end

  include T

  let create chn stream = { chn = Uri.to_string chn; stream = Int32.of_int_exn stream }

  include Comparable.Make (T)
  include Hashable.Make (T)
end

type t =
  { ctx : Context.t
  ; client : Aeron.t
  ; pubs : pub Endpoint.Table.t
  }

and pub = P : 'a Publication.t -> pub [@@deriving fields]

let create ?timeout dir =
  let ctx = Context.create () in
  Context.set_dir ctx dir;
  Option.iter timeout ~f:(fun timeout ->
    Context.set_driver_timeout_ms ctx (Time_ns.Span.to_int_ms timeout));
  let client = init ctx in
  start client;
  let pubs = Endpoint.Table.create () in
  Fields.create ~ctx ~client ~pubs
;;

let close { client; ctx; pubs } =
  let pubs = Hashtbl.to_alist pubs in
  Deferred.List.iter pubs ~how:`Parallel ~f:(fun (_, P pub) -> Publication.close pub)
  >>| fun () ->
  Aeron.close client;
  Aeron.Context.close ctx
;;

let add_publication { client; pubs; _ } kind chan streamID encode =
  let endp = Endpoint.create chan streamID in
  match Hashtbl.find pubs endp, kind with
  | None, `Concurrent ->
    Publication.add_concurrent client chan endp.stream encode
    >>| fun x ->
    Hashtbl.set pubs ~key:endp ~data:(P x);
    `Ok x
  | None, `Exclusive ->
    Publication.add_exclusive client chan endp.stream encode
    >>| fun x ->
    Hashtbl.set pubs ~key:endp ~data:(P x);
    `Ok x
  | Some _, _ -> return `Duplicate
;;

let add_publication_exn t kind chan streamID encode =
  add_publication t kind chan streamID encode
  >>= function
  | `Ok x -> return x
  | `Duplicate ->
    raise_s [%message "duplicate stream" ~chan:(chan : Uri.t) ~id:(streamID : int)]
;;

module Subscription = struct
  type sub =
    { sub : Subscription.t
    ; r : Reader.t
    ; wfd : Fd.t
    }
  [@@deriving fields]

  let create t uri streamID =
    let wait = Aeron.Subscription.add t.client uri (Int.to_int32_exn streamID) in
    Unix.pipe (Info.of_string "aeron_subscription_add_pipe")
    >>= function
    | `Reader rfd, `Writer wfd ->
      let wfd_raw = Fd.file_descr_exn wfd in
      let rec loop () =
        match Aeron.Subscription.add_poll wait wfd_raw with
        | None -> Scheduler.yield () >>= loop
        | Some sub ->
          let r = Reader.create rfd in
          return (Fields_of_sub.create ~sub ~r ~wfd)
      in
      loop ()
  ;;

  let close t =
    Subscription.close t.sub;
    let rec loop () =
      match Subscription.is_closed t.sub with
      | true ->
        (* cleanup *)
        Fd.close t.wfd
      | false -> Scheduler.yield () >>= loop
    in
    loop ()
  ;;
end

let poll_subscription
      ?(stop = Deferred.never ())
      ?(wait = Time_ns.Span.of_int_us 10)
      ?(max_fragments = 10)
      (sub : Subscription.sub)
      f
  =
  (* Repeatedly [max_fragments] till [stop] is determined. *)
  don't_wait_for
    (let rec loop () =
       let _nb_fragments = Aeron.Subscription.poll sub.sub max_fragments in
       if Deferred.is_determined stop
       then (
         Lo.debug (fun m -> m "Closing subscription");
         Subscription.close sub
         >>= fun () ->
         Lo.debug (fun m -> m "Closed subscription");
         Reader.close sub.r)
       else Clock_ns.after wait >>= loop
     in
     loop ());
  let bbuf = Bigbuffer.create 4096 in
  let hdr = Bigstring.create Header.sizeof_values in
  let shdr = Bigsubstring.create hdr in
  let buf = Bigstring.create 4096 in
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
