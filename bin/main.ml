open Core
open Async
open Aeron

let subscribe timeout prefix chan streamID =
  let ctx = create_context () in
  context_set_dir ctx prefix;
  context_set_driver_timeout_ms ctx (Time_ns.Span.to_int_ms timeout);
  let client = init ctx in
  start client;
  Aeron_async.add_subscription client (Uri.of_string chan) (Int32.of_int_exn streamID)
  >>= fun sub ->
  (match subscription_status sub with
   | 1 -> printf "Subscription OK\n"
   | _ -> assert false);
  let terminate = Ivar.create () in
  let cleanup =
    Ivar.read terminate
    >>= fun () ->
    printf "Cleanup up...\n";
    Aeron_async.close_subscription sub
    >>| fun () ->
    close client;
    close_context ctx
  in
  Signal.(handle terminating ~f:(fun _ -> Ivar.fill_if_empty terminate ()));
  cleanup
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
           ~default:"/dev/shm"
           ~doc:"STRING aeron.dir location specified as prefix"
       and chan =
         flag_optional_with_default_doc
           "chan"
           string
           String.sexp_of_t
           ~default:"aeron:udp?endpoint=localhost:40123"
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
         subscribe timeout prefix chan streamID])
;;

let publish timeout prefix chan streamID =
  let ctx = create_context () in
  context_set_dir ctx prefix;
  context_set_driver_timeout_ms ctx (Time_ns.Span.to_int_ms timeout);
  let client = init ctx in
  start client;
  Aeron_async.add_publication client (Uri.of_string chan) (Int32.of_int_exn streamID)
  >>= fun pub ->
  let terminate = Ivar.create () in
  let rec loop i =
    match Ivar.is_full terminate with
    | true -> Deferred.unit
    | false ->
      let msg = Format.kasprintf Bigstring.of_string "Message %i" i in
      let len = Bigstring.length msg in
      (match publication_offer pub msg len with
       | Ok x -> printf "New offset %d\n" x
       | Error x -> Format.printf "%a\n%!" Sexp.pp (sexp_of_offer_result x));
      Clock_ns.after (Time_ns.Span.of_int_sec 1) >>= fun () -> loop (succ i)
  in
  don't_wait_for (loop 0);
  (* Cleanup *)
  let cleanup =
    Ivar.read terminate
    >>= fun () ->
    printf "Cleanup up...\n";
    Aeron_async.close_publication pub
    >>| fun () ->
    close client;
    close_context ctx
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
           ~default:"/dev/shm"
           ~doc:"STRING aeron.dir location specified as prefix"
       and chan =
         flag_optional_with_default_doc
           "chan"
           string
           String.sexp_of_t
           ~default:"aeron:udp?endpoint=localhost:40123"
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
         publish timeout prefix chan streamID])
;;

let cmds =
  Command.group ~summary:"Aeron Armyknife" [ "subscribe", subscribe; "publish", publish ]
;;

let () = Command_unix.run cmds
