open Async
open Aeron

let add_publication t uri streamID =
  let wait = add_publication t uri streamID in
  let rec loop () =
    match add_publication_poll wait with
    | None -> Scheduler.yield () >>= loop
    | Some x -> return x
  in
  loop ()
;;

let add_subscription t uri streamID =
  let wait = add_subscription t uri streamID in
  let rec loop () =
    match add_subscription_poll wait with
    | None -> Scheduler.yield () >>= loop
    | Some x -> return x
  in
  loop ()
;;

let close_publication t =
  close_publication t;
  let rec loop () =
    match publication_is_closed t with
    | true -> Deferred.unit
    | false -> Scheduler.yield () >>= loop
  in
  loop ()
;;

let close_subscription t =
  close_subscription t;
  let rec loop () =
    match subscription_is_closed t with
    | true -> Deferred.unit
    | false -> Scheduler.yield () >>= loop
  in
  loop ()
;;
