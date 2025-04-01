open Core
open Async
open Aeron

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

  let offer ?pos ?len t msg = P.offer ?pos ?len t msg
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
    ; encode : 'a -> Bigstring.t
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

  let offer ?pos ?len { pub; encode } msg =
    match pub with
    | Concurrent x -> ConcurrentPublication.offer ?pos ?len x (encode msg)
    | Exclusive x -> ExclusivePublication.offer ?pos ?len x (encode msg)
  ;;

  let consts { pub; _ } =
    match pub with
    | Concurrent x -> ConcurrentPublication.consts x
    | Exclusive x -> ExclusivePublication.consts x
  ;;
end

module Subscription = struct
  let add t uri streamID =
    let wait = Subscription.add t uri streamID in
    let rec loop () =
      match Subscription.add_poll wait with
      | None -> Scheduler.yield () >>= loop
      | Some x -> return x
    in
    loop ()
  ;;

  let close t =
    Subscription.close t;
    let rec loop () =
      match Subscription.is_closed t with
      | true -> Deferred.unit
      | false -> Scheduler.yield () >>= loop
    in
    loop ()
  ;;

  let poll ?stop t cb =
    let asm, poll = Subscription.mk_poll cb in
    let rec loop () =
      let nb_fragments = poll t 10 in
      (* printf "got %d frags\n" nb_fragments; *)
      match stop with
      | Some iv when Deferred.is_determined iv ->
        FragmentAssembler.free asm;
        Deferred.unit
      | _ -> Clock_ns.after (Time_ns.Span.of_int_ms nb_fragments) >>= loop
    in
    loop ()
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

let subscribe ?stop t ~chan ~streamID f =
  Subscription.add t.client chan (Int32.of_int_exn streamID)
  >>= fun sub ->
  Monitor.protect
    (fun () -> Subscription.poll ?stop sub f)
    ~finally:(fun () -> Subscription.close sub)
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
