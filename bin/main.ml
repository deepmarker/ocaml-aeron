open Core
open Async
open Aeron
open Aeron_async

let src = Logs.Src.create "aeron.main"

module Lo = (val Logs.src_log src : Logs.LOG)
module PersistentAeron = Persistent_connection_kernel.Make (Aeron_async)

let cb iobuf =
  (* Iter of iobuf and assert that each byte contains wrapped
     position. *)
  let len = Iobuf.length iobuf in
  for i = 0 to len - 1 do
    let x = Iobuf.Consume.uint8 iobuf in
    (* Lo.debug (fun m -> m "%d %d %d" i x (i mod 256)); *)
    assert (x = i mod 256)
  done;
  Lo.app (fun m -> m "(%d bytes payload)" len)
;;

let on_error errcode errmsg = Lo.err (fun m -> m "%a: %s" Err.pp errcode errmsg)

let subscribe_direct driver_timeout prefix chan streamID =
  let conn =
    PersistentAeron.create
      ~on_event:(fun evt ->
        let sexp = PersistentAeron.Event.sexp_of_t String.sexp_of_t evt in
        Lo.info (fun m -> m "%a" Sexp.pp sexp);
        Deferred.unit)
      ~address:(module String)
      ~server_name:""
      ~connect:(create ~on_error ?driver_timeout)
      (fun () -> Deferred.Or_error.return prefix)
  in
  let terminate = Ivar.create () in
  Signal.(handle terminating ~f:(fun _ -> Ivar.fill_if_empty terminate ()));
  let handle client =
    Lo.debug (fun m -> m "start handle");
    Subscription.create client chan streamID
    >>= fun sub ->
    Lo.info (fun m -> m "Subscription created");
    Monitor.protect
      (fun () -> Subscription.start_polling_loop ~stop:(Ivar.read terminate) sub cb)
      ~finally:(fun () ->
        Subscription.close sub >>| fun () -> Lo.info (fun m -> m "Cleanup done"))
    >>= fun () ->
    (* Important: wait till client is closed before looping. *)
    if Ivar.is_full terminate then Deferred.unit else close_finished client
  in
  let rec loop () =
    if Ivar.is_full terminate
    then Deferred.unit
    else PersistentAeron.connected conn >>= fun client -> handle client >>= loop
  in
  loop ()
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
         flag
           "driver-timeout"
           (optional Time_ns_unix.Span.arg_type)
           ~doc:"SPAN set driver liveliness timeout"
       and () = Logs_async_reporter.set_level_via_param []
       and () = Logs_async_reporter.set_color_via_param () in
       fun () ->
         Logs.set_reporter (Logs_async_reporter.reporter ());
         let chan = Uri.of_string chan in
         subscribe_direct timeout prefix chan streamID])
;;

let publish driver_timeout dir channel stream sz =
  let channel = Uri.of_string channel in
  let conn =
    PersistentAeron.create
      ~on_event:(fun evt ->
        let sexp = PersistentAeron.Event.sexp_of_t String.sexp_of_t evt in
        Lo.info (fun m -> m "%a" Sexp.pp sexp);
        Deferred.unit)
      ~address:(module String)
      ~server_name:""
      ~connect:(create ~on_error ?driver_timeout)
      (fun () -> Deferred.Or_error.return dir)
  in
  let terminate = Ivar.create () in
  let connect () =
    PersistentAeron.connected conn
    >>= fun client ->
    Lo.info (fun m -> m "Client created");
    Aeron_async.add_publication_exn client `Exclusive channel stream Fn.id
    >>= fun pub ->
    Lo.info (fun m -> m "Exclusive publication created");
    let consts = Publication.consts pub in
    Lo.info (fun m -> m "%a" Sexp.pp (Aeron.sexp_of_pub_consts consts));
    let msg = Iobuf.create ~len:sz in
    for i = 0 to sz - 1 do
      Iobuf.Fill.int8_trunc msg i
    done;
    let msg = Iobuf.read_only msg in
    let rec loop pub i =
      (* Make it ready to consume. *)
      Iobuf.flip_lo msg;
      match is_closed client || Ivar.is_full terminate with
      | true -> Deferred.unit
      | false ->
        (match Publication.offer pub msg with
         | NewStreamPosition x -> Lo.app (fun m -> m "New offset %d" x)
         | x -> Lo.err (fun m -> m "%a" Sexp.pp (Aeron.OfferResult.sexp_of_t x)));
        Clock_ns.after (Time_ns.Span.of_int_sec 1) >>= fun () -> loop pub (succ i)
    in
    loop pub 0
  in
  let rec loop () =
    if Ivar.is_full terminate then Deferred.unit else connect () >>= loop
  in
  Signal.(handle terminating ~f:(fun _ -> Ivar.fill_if_empty terminate ()));
  loop ()
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
         flag
           "driver-timeout"
           (optional Time_ns_unix.Span.arg_type)
           ~doc:"SPAN set driver liveliness timeout"
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
