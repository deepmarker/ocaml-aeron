open Core
open Async
open Aeron

module Publication = struct
  let add t uri streamID =
    let wait = Publication.add t uri streamID in
    let rec loop () =
      match Publication.add_poll wait with
      | None -> Scheduler.yield () >>= loop
      | Some x -> return x
    in
    loop ()
  ;;

  let close t =
    Publication.close t;
    let rec loop () =
      match Publication.is_closed t with
      | true -> Deferred.unit
      | false -> Scheduler.yield () >>= loop
    in
    loop ()
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
end
