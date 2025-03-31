open Core
open Async
open Aeron

let src = Logs.Src.create "aeron.main"

module Lo = (val Logs.src_log src : Logs.LOG)

let cb buf hdr =
  Lo.app (fun m ->
    m "(%a) (%d bytes payload)" Sexp.pp (Header.sexp_of_t hdr) (Bigstring.length buf))
;;

let _subscribe timeout prefix chan streamID =
  let ctx = Context.create () in
  Context.set_dir ctx prefix;
  Context.set_driver_timeout_ms ctx (Time_ns.Span.to_int_ms timeout);
  let client = init ctx in
  start client;
  Aeron_async.Subscription.add client (Uri.of_string chan) (Int32.of_int_exn streamID)
  >>= fun sub ->
  (match Subscription.status sub with
   | 1 -> Lo.info (fun m -> m "Subscription OK")
   | _ -> assert false);
  let consts = Subscription.consts sub in
  Lo.info (fun m -> m "%a" Sexp.pp (sexp_of_consts consts));
  let terminate = Ivar.create () in
  don't_wait_for (Aeron_async.Subscription.poll ~stop:(Ivar.read terminate) sub cb);
  let cleanup =
    Ivar.read terminate
    >>= fun () ->
    Aeron_async.Subscription.close sub
    >>| fun () ->
    close client;
    Context.close ctx;
    Lo.info (fun m -> m "Cleanup done")
  in
  Signal.(handle terminating ~f:(fun _ -> Ivar.fill_if_empty terminate ()));
  cleanup
;;

let subscribe_direct timeout prefix chan streamID =
  let ctx = Context.create () in
  Context.set_dir ctx prefix;
  Context.set_driver_timeout_ms ctx (Time_ns.Span.to_int_ms timeout);
  let client = init ctx in
  start client;
  let terminate = Ivar.create () in
  Signal.(handle terminating ~f:(fun _ -> Ivar.fill_if_empty terminate ()));
  Aeron_async.Subscription.subscribe_direct
    ~stop:(Ivar.read terminate)
    client
    ~chan
    ~streamID
    cb
  >>| fun () ->
  close client;
  Context.close ctx;
  Lo.info (fun m -> m "Cleanup done")
;;

let subscribe =
  Command.async
    ~summary:"Basic subscriber"
    (let open Command.Let_syntax in
     [%map_open
       let prefix =
         flag_optional_with_default_doc
           "p"
           string
           String.sexp_of_t
           ~default:("/dev/shm/aeron-" ^ Sys.getenv_exn "LOGNAME")
           ~doc:"STRING aeron.dir location specified as prefix"
       and chan =
         flag_optional_with_default_doc
           "chan"
           string
           sexp_of_string
           ~default:"aeron:ipc"
           ~doc:"URI default channel to subscribe to"
       and streamID =
         flag_optional_with_default_doc
           "s"
           int
           Int.sexp_of_t
           ~default:10
           ~doc:"INT stream-id to use"
       and timeout =
         flag_optional_with_default_doc
           "to"
           Time_ns_unix.Span.arg_type
           Time_ns_unix.Span.sexp_of_t
           ~default:(Time_ns.Span.of_int_sec 10)
           ~doc:"SPAN driver liveliness timeout"
       and () = Logs_async_reporter.set_level_via_param []
       and () = Logs_async_reporter.set_color_via_param () in
       fun () ->
         Logs.set_reporter (Logs_async_reporter.reporter ());
         let chan = Uri.of_string chan in
         subscribe_direct timeout prefix chan streamID])
;;

let publish timeout dir channel stream sz =
  let open Aeron_async.Publication in
  let channel = Uri.of_string channel in
  let stream = Int32.of_int_exn stream in
  EZ.create dir ~timeout channel stream
  >>= fun conn ->
  let consts = Publication.consts conn.v in
  Lo.info (fun m -> m "%a" Sexp.pp (sexp_of_consts consts));
  let terminate = Ivar.create () in
  let rec loop i =
    match Ivar.is_full terminate with
    | true -> Deferred.unit
    | false ->
      let msg = Bigstring.create sz in
      (match EZ.offer conn msg with
       | NewStreamPosition x -> Lo.app (fun m -> m "New offset %d" x)
       | x -> Lo.err (fun m -> m "%a" Sexp.pp (OfferResult.sexp_of_t x)));
      Clock_ns.after (Time_ns.Span.of_int_sec 1) >>= fun () -> loop (succ i)
  in
  don't_wait_for (loop 0);
  (* Cleanup *)
  let cleanup =
    Ivar.read terminate
    >>= fun () -> EZ.close conn >>| fun () -> Lo.info (fun m -> m "Cleanup done")
  in
  Signal.(handle terminating ~f:(fun _ -> Ivar.fill_if_empty terminate ()));
  cleanup
;;

let publish =
  Command.async
    ~summary:"Basic subscriber"
    (let open Command.Let_syntax in
     [%map_open
       let prefix =
         flag_optional_with_default_doc
           "p"
           string
           String.sexp_of_t
           ~default:("/dev/shm/aeron-" ^ Sys.getenv_exn "LOGNAME")
           ~doc:"STRING aeron.dir location specified as prefix"
       and chan =
         flag_optional_with_default_doc
           "chan"
           string
           String.sexp_of_t
           ~default:"aeron:ipc"
           ~doc:"URI default channel to publish to"
       and streamID =
         flag_optional_with_default_doc
           "s"
           int
           Int.sexp_of_t
           ~default:10
           ~doc:"INT stream-id to use"
       and timeout =
         flag_optional_with_default_doc
           "to"
           Time_ns_unix.Span.arg_type
           Time_ns_unix.Span.sexp_of_t
           ~default:(Time_ns.Span.of_int_sec 10)
           ~doc:"SPAN driver liveliness timeout"
       and sz =
         flag_optional_with_default_doc
           "sz"
           int
           sexp_of_int
           ~default:10
           ~doc:"INT Size of messages sent"
       and () = Logs_async_reporter.set_level_via_param []
       and () = Logs_async_reporter.set_color_via_param () in
       fun () ->
         Logs.set_reporter (Logs_async_reporter.reporter ());
         publish timeout prefix chan streamID sz])
;;

let cmds =
  Command.group ~summary:"Aeron Armyknife" [ "subscribe", subscribe; "publish", publish ]
;;

let () = Command_unix.run cmds
