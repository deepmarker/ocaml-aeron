open Core
open Async
open Alcotest
open Alcotest_async

(* Aeron's add and close complete asynchronously at the media driver, so
   they are polled. Before this was bounded, a driver that stopped
   answering -- as it does when the machine suspends and the client times
   out -- was polled forever: the add never completed, the persistent
   connection wrapping the publication never saw an attempt fail, and every
   offer waiting behind it hung indefinitely. One such hang froze a
   downstream service for 21 hours. *)

let span = Time_ns.Span.of_int_ms

let test_returns_immediately () =
  Aeron_async.poll_until ~what:"ready" (fun () -> Some 42)
  >>| function
  | Ok v -> check int "value" 42 v
  | Error err -> fail (Error.to_string_hum err)
;;

let test_polls_until_ready () =
  let calls = ref 0 in
  Aeron_async.poll_until ~what:"eventually" (fun () ->
    incr calls;
    Option.some_if (!calls >= 3) !calls)
  >>| function
  | Ok v ->
    check bool "polled more than once" true (v >= 3);
    check bool "did not overshoot wildly" true (v < 100)
  | Error err -> fail (Error.to_string_hum err)
;;

(* The point of the change: a driver that never answers must produce an
   error, and must produce it promptly, rather than a deferred that is
   never determined. *)
let test_gives_up_on_an_unresponsive_driver () =
  let started = Time_ns.now () in
  Aeron_async.poll_until ~timeout:(span 100) ~what:"add publication" (fun () -> None)
  >>| function
  | Ok () -> fail "polling an unresponsive driver should not succeed"
  | Error err ->
    let waited = Time_ns.diff (Time_ns.now ()) started in
    check
      bool
      "gave up"
      true
      (String.is_substring
         (Error.to_string_hum err)
         ~substring:"did not complete in time");
    check
      bool
      "names the operation"
      true
      (String.is_substring (Error.to_string_hum err) ~substring:"add publication");
    check bool "bounded by the timeout" true Time_ns.Span.(waited < span 5_000)
;;


(* The shared poll loop. Every subscription in a process is drained by
   this one loop rather than by a timer each, and it only idles when a
   pass came back with nothing -- see [Aeron_async.add_subscription]. *)
module Poller = Aeron_async.Poller

let after_ms n = Clock_ns.after (span n)

(* An idle subscription costs one poll per period, no matter how busy the
   scheduler is around it. *)
let test_idle_polls_once_per_period () =
  let polls = ref 0 in
  let stop = ref false in
  Poller.register ~period:(span 10) (fun () ->
    incr polls;
    if !stop then Poller.Finished else Poller.Continue);
  after_ms 100
  >>| fun () ->
  stop := true;
  check bool "polled" true (!polls > 2);
  (* 100ms of 10ms periods is ~10 passes. A loop that re-ran every
     scheduler cycle instead would be in the thousands. *)
  check bool "did not spin" true (!polls < 60)
;;

(* The loop must never poll faster than its period, however much there is
   to read. Fragments reach OCaml through a 64K pipe that a blocking C
   callback fills while holding the runtime lock, and the only reader is
   the Async thread running this loop -- so a loop that re-polls without
   idling can overrun the pipe and deadlock the process against itself.
   That is not hypothetical: it hung the rftp bridge on its first burst. *)
let test_never_polls_faster_than_its_period () =
  let polls = ref 0 in
  let stop = ref false in
  Poller.register ~period:(span 20) (fun () ->
    incr polls;
    if !stop then Poller.Finished else Poller.Continue);
  after_ms 200
  >>| fun () ->
  stop := true;
  (* 200ms of 20ms periods is ~10 passes, whatever the polls report. *)
  check bool "polled" true (!polls > 3);
  check bool "did not outrun the period" true (!polls < 30)
;;

(* Registrants share the loop, so N subscriptions cost N polls per period
   rather than N timers. *)
let test_registrants_share_one_loop () =
  let polls = Array.create ~len:5 0 in
  let stop = ref false in
  Array.iteri polls ~f:(fun i _ ->
    Poller.register ~period:(span 10) (fun () ->
      polls.(i) <- polls.(i) + 1;
      if !stop then Poller.Finished else Poller.Continue));
  after_ms 100
  >>| fun () ->
  stop := true;
  let lo = Array.min_elt polls ~compare |> Option.value_exn in
  let hi = Array.max_elt polls ~compare |> Option.value_exn in
  check bool "all were polled" true (lo > 2);
  check bool "in lockstep, on one timer" true (hi - lo <= 1)
;;

(* A finished subscription stops being polled, and the loop parks once the
   last one goes. *)
let test_finished_is_dropped () =
  let live = ref 0 in
  let dead = ref 0 in
  Poller.register ~period:(span 10) (fun () ->
    incr dead;
    Poller.Finished);
  Poller.register ~period:(span 10) (fun () ->
    incr live;
    Poller.Continue);
  after_ms 100
  >>| fun () ->
  check int "polled once, then dropped" 1 !dead;
  check bool "the other kept going" true (!live > 2)
;;

let () =
  Async.Thread_safe.block_on_async_exn (fun () ->
    run
      "aeron_async"
      [ ( "poll_until"
        , [ test_case "returns immediately" `Quick test_returns_immediately
          ; test_case "polls until ready" `Quick test_polls_until_ready
          ; test_case
              "gives up on an unresponsive driver"
              `Quick
              test_gives_up_on_an_unresponsive_driver
          ] )
      ; ( "shared poller"
        , [ test_case "an idle subscription polls once per period" `Quick
              test_idle_polls_once_per_period
          ; test_case "never polls faster than its period" `Quick
              test_never_polls_faster_than_its_period
          ; test_case "registrants share one loop" `Quick test_registrants_share_one_loop
          ; test_case "a finished subscription is dropped" `Quick test_finished_is_dropped
          ] )
      ])
;;
