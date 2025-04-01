open Core
open Async

let src = Logs.Src.create "aeron.main"

module Lo = (val Logs.src_log src : Logs.LOG)

let cb buf hdr =
  let open Aeron in
  Lo.app (fun m ->
    m "(%a) (%d bytes payload)" Sexp.pp (Header.sexp_of_t hdr) (Bigstring.length buf))
;;

let subscribe_direct timeout prefix chan streamID =
  let client = Aeron_async.create ~timeout prefix in
  let terminate = Ivar.create () in
  Signal.(handle terminating ~f:(fun _ -> Ivar.fill_if_empty terminate ()));
  Aeron_async.subscribe ~stop:(Ivar.read terminate) client ~chan ~streamID cb
  >>= fun () -> Aeron_async.close client >>| fun () -> Lo.info (fun m -> m "Cleanup done")
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
  let open Aeron_async in
  let channel = Uri.of_string channel in
  let client = Aeron_async.create ~timeout dir in
  Aeron_async.add_publication_exn client `Exclusive channel stream Fn.id
  >>= fun pub ->
  let consts = Publication.consts pub in
  Lo.info (fun m -> m "%a" Sexp.pp (Aeron.sexp_of_consts consts));
  let terminate = Ivar.create () in
  let rec loop i =
    match Ivar.is_full terminate with
    | true -> Deferred.unit
    | false ->
      let msg = Bigstring.create sz in
      (match Publication.offer pub msg with
       | NewStreamPosition x -> Lo.app (fun m -> m "New offset %d" x)
       | x -> Lo.err (fun m -> m "%a" Sexp.pp (Aeron.OfferResult.sexp_of_t x)));
      Clock_ns.after (Time_ns.Span.of_int_sec 1) >>= fun () -> loop (succ i)
  in
  don't_wait_for (loop 0);
  (* Cleanup *)
  let cleanup =
    Ivar.read terminate
    >>= fun () ->
    (* This will cleanup the publication. *)
    Aeron_async.close client >>| fun () -> Lo.info (fun m -> m "Cleanup done")
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
