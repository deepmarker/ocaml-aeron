(* Manual, not-run-in-CI check against a real [aeronmd]: spins up the actual
   media driver, wires a [Persistent.Client] publication and subscription
   over it, kills the driver mid-stream (the same failure a host suspend
   produces -- the client's conductor hits a driver timeout), restarts it,
   and confirms the publication/subscription come back on their own instead
   of staying silently dead. Not wired into the dune test runner: it forks
   real [aeronmd] processes and needs generous wall-clock waits for driver
   timeouts, which doesn't belong in `dune runtest`. Run with:

     dune exec lib/aeron/test/manual_persistent_driver_test.exe -- <aeron-dir>
*)
open Core
open Async

let src = Logs.Src.create "manual_persistent_driver_test"

module Lo = (val Logs.src_log src : Logs.LOG)

let driver_timeout = Time_ns.Span.of_int_sec 2

let spawn_driver ?(force_recreate = false) dir =
  let args =
    [ "-Daeron.dir=" ^ dir ]
    @
    if force_recreate
    then
      (* Without this, a driver killed (rather than shut down cleanly)
         leaves a heartbeat in cnc.dat that a driver started on the same
         dir considers "active" for ~10s and refuses to touch --
         "Device or resource busy". Force past that so the test doesn't
         have to sleep out the driver's own liveness window. *)
      [ "-Daeron.dir.delete.on.start=true" ]
    else []
  in
  Process.create_exn ~prog:"aeronmd" ~args ()
;;

let wait_for_file ?(timeout = Time_ns.Span.of_int_sec 10) path =
  let deadline = Time_ns.add (Time_ns.now ()) timeout in
  let rec loop () =
    if Sys_unix.file_exists_exn path
    then Deferred.unit
    else if Time_ns.( > ) (Time_ns.now ()) deadline
    then failwithf "timed out waiting for %s" path ()
    else Clock_ns.after (Time_ns.Span.of_int_ms 100) >>= loop
  in
  loop ()
;;

(* Retry [f] until it returns [Some _] or [timeout] elapses -- used both to
   wait for a message to arrive at the subscriber and to wait for a
   publication offer to succeed again after a reconnect, since neither
   happens on a fixed schedule. Bounds the whole loop with
   [Clock_ns.with_timeout] rather than checking a deadline between
   iterations: a single [f ()] call that never resolves (which is exactly
   the kind of regression this harness exists to catch) must not defeat the
   timeout by never letting the loop check it. *)
let retry ?(timeout = Time_ns.Span.of_int_sec 15) ~what f =
  let rec loop () =
    f ()
    >>= function
    | Some x -> return x
    | None -> Clock_ns.after (Time_ns.Span.of_int_ms 200) >>= loop
  in
  Clock_ns.with_timeout timeout (loop ())
  >>| function
  | `Result x -> x
  | `Timeout -> failwithf "timed out waiting for: %s" what ()
;;

(* Keeps offering and waiting for delivery cleanly separate: re-offering on
   every poll tick until a message is *seen* (rather than stopping once the
   offer itself succeeds) sends duplicates whenever delivery lags one tick
   behind the offer, and a stray duplicate left in [received] then corrupts
   whichever check runs next. *)
let offer_until_ok ~timeout pub msg =
  retry ~timeout ~what:(sprintf "offer %s succeeds" msg) (fun () ->
    Aeron_async.Persistent.offer pub msg
    >>| function
    | Error err ->
      Lo.app (fun m -> m "offer not ready yet: %a" Error.pp err);
      None
    | Ok (Error offer_err) ->
      Lo.app (fun m ->
        m "offer error: %s" (Sexp.to_string_hum (Aeron.OfferError.sexp_of_t offer_err)));
      None
    | Ok (Ok (n : int)) ->
      Lo.app (fun m -> m "offer succeeded (pos %d)" n);
      Some ())
;;

let wait_for_message ~timeout received =
  retry ~timeout ~what:"message delivered" (fun () -> return (Queue.dequeue received))
;;

let main dir () =
  Core_unix.mkdir_p dir;
  let received = Queue.create () in
  Lo.app (fun m -> m "=== starting aeronmd #1 in %s ===" dir);
  spawn_driver dir
  >>= fun driver1 ->
  wait_for_file (dir ^/ "cnc.dat")
  >>= fun () ->
  Lo.app (fun m -> m "driver up (pid %s)" (Pid.to_string (Process.pid driver1)));
  let chan = Uri.of_string "aeron:ipc" in
  let streamID = 4200l in
  let client = Aeron_async.Persistent.Client.create ~driver_timeout dir in
  let encoder = Aeron_async.Encoder.alloc Bigstring.of_string in
  let on_msg buf =
    let s = Iobuf.to_string buf in
    Lo.app (fun m -> m "subscriber got: %s" s);
    Queue.enqueue received s
  in
  Aeron_async.Persistent.add_exclusive_publication client chan ~streamID encoder
  >>= fun pub_res ->
  let pub, _consts = Or_error.ok_exn pub_res in
  Aeron_async.Persistent.add_subscription client chan ~streamID on_msg
  >>= fun sub_res ->
  let (_ : Aeron_async.Persistent.subscription), _consts = Or_error.ok_exn sub_res in
  (* Give the pub/sub images time to find each other over IPC before the
     first offer. *)
  Clock_ns.after (Time_ns.Span.of_int_ms 500)
  >>= fun () ->
  Lo.app (fun m -> m "=== phase 1: publish/subscribe against a live driver ===");
  offer_until_ok ~timeout:(Time_ns.Span.of_int_sec 15) pub "hello-1"
  >>= fun () ->
  wait_for_message ~timeout:(Time_ns.Span.of_int_sec 15) received
  >>= fun msg ->
  if String.equal msg "hello-1"
  then Lo.app (fun m -> m "PASS: phase 1 (%s)" msg)
  else failwithf "phase 1: unexpected message %s" msg ();
  Lo.app (fun m ->
    m "=== killing aeronmd #1 (pid %s) ===" (Pid.to_string (Process.pid driver1)));
  Process.send_signal driver1 Signal.kill;
  Process.wait driver1
  >>= fun (_ : Unix.Exit_or_signal.t) ->
  (* Past [driver_timeout] the client's conductor must see the driver is
     gone and rebuild itself; give it a comfortable margin. *)
  Clock_ns.after (Time_ns.Span.scale driver_timeout 2.)
  >>= fun () ->
  Lo.app (fun m -> m "=== starting aeronmd #2 (same dir) ===");
  spawn_driver ~force_recreate:true dir
  >>= fun driver2 ->
  wait_for_file (dir ^/ "cnc.dat")
  >>= fun () ->
  Lo.app (fun m -> m "driver up (pid %s)" (Pid.to_string (Process.pid driver2)));
  Lo.app (fun m ->
    m "=== phase 2: publication/subscription must reconnect on their own ===");
  offer_until_ok ~timeout:(Time_ns.Span.of_int_sec 30) pub "hello-2"
  >>= fun () ->
  wait_for_message ~timeout:(Time_ns.Span.of_int_sec 15) received
  >>= fun msg ->
  if String.equal msg "hello-2"
  then Lo.app (fun m -> m "PASS: phase 2 (%s) -- survived the driver dying" msg)
  else failwithf "phase 2: unexpected message %s" msg ();
  Aeron_async.Persistent.Client.close client
  >>= fun () ->
  Process.send_signal driver2 Signal.kill;
  Process.wait driver2
  >>| fun (_ : Unix.Exit_or_signal.t) -> Lo.app (fun m -> m "=== all phases passed ===")
;;

let () =
  Logs.set_reporter (Logs.format_reporter ());
  (* [Debug], the most verbose: [App] alone (Logs's ordering runs
     App < Error < Warning < Info < Debug) would filter out every [Lo.err]
     [Aeron_async] itself logs on reconnect, which is the whole point of
     watching this run. *)
  Logs.set_level (Some Logs.Debug);
  let dir =
    match Sys.get_argv () with
    | [| _; dir |] -> dir
    | _ -> "/tmp/aeron-persistent-test"
  in
  Async.Thread_safe.block_on_async_exn (main dir)
;;
