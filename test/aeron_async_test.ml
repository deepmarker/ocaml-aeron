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
      ])
;;
