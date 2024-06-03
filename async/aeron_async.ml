open Core
open Async
open Aeron

type endpoint =
  { dir : string
  ; channel : Uri.t
  ; stream : int32
  ; timeout : Time_ns.Span.t option
  }
[@@deriving fields]

type 'a t =
  { ctx : Context.t
  ; client : Aeron.t
  ; v : 'a
  }
[@@deriving fields]

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

  module EZ = struct
    let create ?timeout dir channel stream =
      let ctx = Context.create () in
      Context.set_dir ctx dir;
      Option.iter timeout ~f:(fun timeout ->
        Context.set_driver_timeout_ms ctx (Time_ns.Span.to_int_ms timeout));
      let client = init ctx in
      start client;
      add client channel stream >>| fun pub -> Fields.create ~ctx ~client ~v:pub
    ;;

    let create_endpoint t = create t.dir ?timeout:t.timeout t.channel t.stream

    let close t =
      close t.v
      >>| fun () ->
      Aeron.close t.client;
      Aeron.Context.close t.ctx
    ;;

    let offer ?pos ?len t msg = P.offer ?pos ?len t.v msg
  end
end

module type Publication_sig = sig
  type pub

  val add : Aeron.t -> Uri.t -> int32 -> pub Deferred.t
  val close : pub -> unit Deferred.t

  module EZ : sig
    val create : ?timeout:Time_ns.Span.t -> string -> Uri.t -> int32 -> pub t Deferred.t
    val create_endpoint : endpoint -> pub t Deferred.t
    val close : pub t -> unit Deferred.t
    val offer : ?pos:int -> ?len:int -> pub t -> Bigstring.t -> OfferResult.t
  end
end

module Publication = MkPublication (Publication)
module ExclusivePublication = MkPublication (ExclusivePublication)

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
    let assembler = FragmentAssembler.create cb in
    let rec loop () =
      let nb_fragments = Subscription.poll t assembler 10 in
      (* printf "got %d frags\n" nb_fragments; *)
      match stop with
      | Some iv when Deferred.is_determined iv -> Deferred.unit
      | _ -> Clock_ns.after (Time_ns.Span.of_int_ms nb_fragments) >>= loop
    in
    loop ()
  ;;

  module EZ = struct
    let create ?timeout dir channel stream =
      let ctx = Context.create () in
      Context.set_dir ctx dir;
      Option.iter timeout ~f:(fun timeout ->
        Context.set_driver_timeout_ms ctx (Time_ns.Span.to_int_ms timeout));
      let client = init ctx in
      start client;
      add client channel stream >>| fun sub -> Fields.create ~ctx ~client ~v:sub
    ;;

    let create_endpoint endpoint =
      create ?timeout:endpoint.timeout endpoint.dir endpoint.channel endpoint.stream
    ;;

    let close t =
      close t.v
      >>| fun () ->
      Aeron.close t.client;
      Aeron.Context.close t.ctx
    ;;

    let poll ?stop t cb = poll ?stop t.v cb
  end
end
