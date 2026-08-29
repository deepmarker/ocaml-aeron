open Core
open Async
open Aeron

let src = Logs.Src.create "aeron.async"

module Lo = (val Logs.src_log src : Logs.LOG)

module Encoder = struct
  type ('a, _) t =
    | Alloc : ('a -> Bigstring.t) -> ('a, [ `Alloc ]) t
    | Direct :
        { claim : claim
        ; sizer : 'a -> int
        ; f : Bigstring.t -> 'a -> unit
        }
        -> ('a, [ `Direct ]) t

  let alloc a = Alloc a
  let direct sizer f = Direct { sizer; f; claim = alloc_claim () }
end

(* Aeron's add and close are asynchronous at the driver: the call hands
   back a token, and the operation only completes once the media driver has
   acted on it. When the driver stops acting -- it drops its clients across
   a machine suspend, leaving "client timeout from driver" behind -- the
   token never resolves.

   Polling for that without a deadline turns into an unbounded wait in every
   caller. Worse, it defeats the point of wrapping a publication in a
   [Persistent_connection]: that machinery reconnects and backs off on the
   strength of connect attempts that *fail*, and an attempt that can only
   succeed or hang gives it nothing to act on, so [connected_or_failed_to_connect]
   never reports a failure and every [offer] behind it waits forever.

   Polling on a timer rather than [Scheduler.yield] also matters: yielding
   re-runs the loop every scheduler cycle, so an operation that never
   completes spins as fast as Async will let it. *)
let poll_period = Time_ns.Span.of_int_ms 1
let default_op_timeout = Time_ns.Span.of_int_sec 5

let poll_until ?(timeout = default_op_timeout) ~what f =
  let deadline = Time_ns.add (Time_ns.now ()) timeout in
  let rec loop () =
    match f () with
    | Some x -> Deferred.Or_error.return x
    | None when Time_ns.( > ) (Time_ns.now ()) deadline ->
      Deferred.Or_error.error_s
        [%message
          "aeron: operation did not complete in time; media driver unresponsive?"
            ~(what : string)
            ~(timeout : Time_ns.Span.t)]
    | None -> Clock_ns.after poll_period >>= loop
  in
  loop ()
;;

module type S = sig
  type t

  (** Bounded: reports an error rather than waiting on a driver that has
      stopped answering, so that a persistent connection can see the attempt
      fail, back off and retry. *)
  val add : Aeron.t -> Uri.t -> int32 -> t Deferred.Or_error.t

  val is_closed : t -> bool
  val close_finished : t -> unit Deferred.t
  val close : t -> unit Deferred.t
  val offer : t -> ?pos:int -> ?len:int -> Bigstringaf.t -> (int, OfferError.t) result
  val tryclaim : t -> int -> claim -> (int, OfferError.t) result
  val consts : t -> pub_consts
  val is_connected : t -> bool

  (** Marks [t] closed without touching the underlying C object, unlike
      [close]. For when the client this publication was added through has
      already died and force-closed everything it owns: there is nothing
      left to ask the driver to close, and doing so anyway risks operating
      on a publication the dead client's conductor may already be tearing
      down. This is the only way [connected_or_failed_to_connect] finds out
      to reconnect when nothing else calls [offer]/[tryclaim] in the
      meantime to notice on its own (see [Persistent.Client]). *)
  val invalidate : t -> unit
end

module MkAsyncPublication (P : Publication_sig) : S = struct
  type t =
    { pub : P.t
    ; closed : unit Ivar.t
    }

  let add t uri streamID =
    let wait = P.add t uri streamID in
    poll_until ~what:"add publication" (fun () -> P.add_poll wait)
    >>|? fun pub -> { pub; closed = Ivar.create () }
  ;;

  let is_closed { pub; _ } = P.is_closed pub
  let close_finished { closed; _ } = Ivar.read closed
  let invalidate { closed; _ } = Ivar.fill_if_empty closed ()

  (* idempotent. *)
  let close { pub; closed } =
    match Ivar.is_full closed with
    | true -> Deferred.unit
    | false ->
      P.close pub;
      poll_until ~what:"close publication" (fun () -> Option.some_if (P.is_closed pub) ())
      >>| fun res ->
      (* The driver has been told to close either way; refusing to give up
         here would only hang shutdown. *)
      (match res with
       | Ok () -> ()
       | Error err -> Lo.err (fun m -> m "%a" Error.pp err));
      Ivar.fill_if_empty closed ()
  ;;

  let offer { pub; closed } ?pos ?len buf =
    let res = P.offer ?pos ?len pub buf in
    match res with
    | Error Closed ->
      Ivar.fill_if_empty closed ();
      res
    | _ -> res
  ;;

  let tryclaim { pub; closed } i claim =
    let res = P.tryclaim pub i claim in
    match res with
    | Error Closed ->
      Ivar.fill_if_empty closed ();
      res
    | _ -> res
  ;;

  let consts { pub; _ } = P.consts pub
  let is_connected { pub; _ } = P.is_connected pub
end

module MkPublication (S : S) = struct
  module PPub = Persistent_connection_kernel.Make (S)

  type ('a, 'b) t =
    { pub : PPub.t
    ; encode : ('a, 'b) Encoder.t
    }

  module Address = struct
    type t =
      { chan : Uri_sexp.t
      ; stream_id : int32
      }
    [@@deriving sexp, compare, equal]
  end

  (* Add a connection. [get_client] is consulted on every (re)connect
     attempt rather than once up front, so a publication added on top of a
     [Persistent.Client.t] follows that client across a reconnect instead of
     retrying [S.add] forever against a client whose conductor is dead.

     [get_client] also hands back that client's own death signal, which
     [invalidate]s the publication once it fires. Without this, a
     publication that never needed to notice a failure on its own (because
     nothing called [offer]/[tryclaim] between the client dying and coming
     back) would wait forever: [connected_or_failed_to_connect] won't hand
     out a connection it already sees as closed, but nothing ever prompts a
     reconnect either, since that only happens once *this* wrapper's own
     [close_finished] fires -- which, absent this hook, only [offer]
     noticing [Error Closed] can do. Confirmed live: without it, a
     publication added before a driver restart never reconnects, no matter
     how many offers are retried against it afterwards. *)
  let create ~get_client chan stream_id encode =
    let pub =
      PPub.create
        ~server_name:""
        ~address:(module Address)
        ~connect:(fun { Address.chan; stream_id } ->
          get_client ()
          >>=? fun (client, client_dead) ->
          Monitor.try_with_or_error (fun () -> S.add client chan stream_id)
          >>| Or_error.join
          >>|? fun x ->
          don't_wait_for (client_dead >>| fun () -> S.invalidate x);
          x)
        (fun () -> Deferred.Or_error.return { Address.chan; stream_id })
    in
    { pub; encode }
  ;;

  let consts { pub; _ } = PPub.connected_or_failed_to_connect pub >>|? S.consts

  (* Non-blocking, unlike [consts]/[offer]: [None] while no connect attempt
     has resolved yet (the persistent publication is still retrying, e.g.
     against a client that hasn't reconnected). Exists for exactly the
     situation where waiting is the problem being diagnosed --
     [connected_or_failed_to_connect] would just join the same stuck
     future [offer_bounded] is timing out on. *)
  let is_connected_now { pub; _ } = Option.map (PPub.current_connection pub) ~f:S.is_connected

  let offer { pub; _ } ?pos ?len s =
    PPub.connected_or_failed_to_connect pub >>|? fun x -> S.offer x ?pos ?len s
  ;;

  let tryclaim { pub; _ } i claim =
    PPub.connected_or_failed_to_connect pub >>|? fun x -> S.tryclaim x i claim
  ;;

  let handle_direct pub sizer f claim msg =
    let len = sizer msg in
    tryclaim pub len claim
    >>|? function
    | Error err -> Result.fail err
    | Ok newpos ->
      let bs = bigstring_of_claim claim in
      f bs msg;
      if commit_claim claim <> 0 then failwith "commit claim failed";
      Result.return newpos
  ;;

  (* Like [offer]/[tryclaim], but check [abandoned] right after the
     connection resolves and before actually calling [S.offer]/[S.tryclaim]
     -- unlike [Clock_ns.with_timeout], which only stops a caller from
     *waiting* on this bind, not the bind itself: it still fires, and still
     sends, whenever the publication next reconnects. [offer_bounded] fills
     [abandoned] exactly when it gives up, so an attempt it already gave up
     on does not silently deliver a stale message later, out of order
     relative to whatever the caller sent instead in the meantime. Returns
     [None] for "abandoned before it could send" as distinct from an actual
     [S.offer]/[S.tryclaim] result. *)
  let offer_or_abandon { pub; _ } ~abandoned ?pos ?len s =
    PPub.connected_or_failed_to_connect pub
    >>|? fun x -> if Ivar.is_full abandoned then None else Some (S.offer x ?pos ?len s)
  ;;

  let tryclaim_or_abandon { pub; _ } ~abandoned i claim =
    PPub.connected_or_failed_to_connect pub
    >>|? fun x -> if Ivar.is_full abandoned then None else Some (S.tryclaim x i claim)
  ;;

  let handle_direct_or_abandon pub ~abandoned sizer f claim msg =
    let len = sizer msg in
    tryclaim_or_abandon pub ~abandoned len claim
    >>|? function
    | None -> None
    | Some (Error err) -> Some (Result.fail err)
    | Some (Ok newpos) ->
      let bs = bigstring_of_claim claim in
      f bs msg;
      if commit_claim claim <> 0 then failwith "commit claim failed";
      Some (Result.return newpos)
  ;;
end

module Concurrent = MkPublication (MkAsyncPublication (Publication))
module Exclusive = MkPublication (MkAsyncPublication (ExclusivePublication))

type subscription =
  { sub : Subscription.t
  ; r : Reader.t
  }
[@@deriving fields]

exception Stopped

type ('a, 'b) publication =
  | Concurrent of ('a, 'b) Concurrent.t
  | Exclusive of ('a, 'b) Exclusive.t

let close_publication = function
  | Concurrent { pub; _ } -> Concurrent.PPub.close pub
  | Exclusive { pub; _ } -> Exclusive.PPub.close pub
;;

let is_connected_now : type a b. (a, b) publication -> bool option = function
  | Concurrent p -> Concurrent.is_connected_now p
  | Exclusive p -> Exclusive.is_connected_now p
;;

type t =
  { ctx : Context.t
  ; ba : Bigstring.t
    (* useless here but must not be GCed, contains fd information for
       C -> OCaml errors. *)
  ; client : Aeron.t
  ; pubs : pub Int64.Table.t
  ; stop : unit Ivar.t
  ; subs : subscription Int64.Table.t
  }

and pub = P : ('a, 'b) publication -> pub [@@deriving fields]

(* The actual dispatch does not need a client: [Concurrent.offer] /
   [Exclusive.offer] already go through the publication's own persistent
   connection ([PPub.connected_or_failed_to_connect]), which reports a dead
   connection as an [Error] rather than needing a specific [t] to ask. Shared
   by the raw [offer] below (which additionally checks that its owning
   client hasn't been explicitly stopped) and by [Persistent.offer] (whose
   publications aren't tied to a single client). *)
let offer_pub
  : type a b. (a, b) publication -> a -> (int, OfferError.t) result Deferred.Or_error.t
  =
  fun pub msg ->
  match pub with
  | Concurrent ({ encode = Alloc f; _ } as pub) -> Concurrent.offer pub (f msg)
  | Concurrent ({ encode = Direct { sizer; f; claim }; _ } as pub) ->
    Concurrent.handle_direct pub sizer f claim msg
  | Exclusive ({ encode = Alloc f; _ } as pub) -> Exclusive.offer pub (f msg)
  | Exclusive ({ encode = Direct { sizer; f; claim }; _ } as pub) ->
    Exclusive.handle_direct pub sizer f claim msg
;;

(* Mirrors [offer_pub], but through [offer_or_abandon]/[handle_direct_or_abandon]
   -- see those for why [offer_bounded] needs this instead of racing
   [offer_pub] against a timeout from the outside. *)
let offer_pub_or_abandon
  : type a b.
    (a, b) publication
    -> abandoned:unit Ivar.t
    -> a
    -> (int, OfferError.t) result option Deferred.Or_error.t
  =
  fun pub ~abandoned msg ->
  match pub with
  | Concurrent ({ encode = Alloc f; _ } as pub) -> Concurrent.offer_or_abandon pub ~abandoned (f msg)
  | Concurrent ({ encode = Direct { sizer; f; claim }; _ } as pub) ->
    Concurrent.handle_direct_or_abandon pub ~abandoned sizer f claim msg
  | Exclusive ({ encode = Alloc f; _ } as pub) -> Exclusive.offer_or_abandon pub ~abandoned (f msg)
  | Exclusive ({ encode = Direct { sizer; f; claim }; _ } as pub) ->
    Exclusive.handle_direct_or_abandon pub ~abandoned sizer f claim msg
;;

let offer
  : type a b.
    t -> (a, b) publication -> a -> (int, OfferError.t) result Deferred.Or_error.t
  =
  fun t pub msg ->
  if Ivar.is_full t.stop then raise Stopped;
  offer_pub pub msg
;;

let close { client; ctx; pubs; stop; subs; _ } =
  match Ivar.is_full stop with
  | true ->
    (* Already closed! Not idempotent! *)
    Deferred.unit
  | false ->
    (* Signaling the start of closing? This will trigger a reconnect
     when using persistent connection. *)
    Ivar.fill_if_empty stop ();
    Lo.debug (fun m -> m "start closing aeron client");
    let pubs = Hashtbl.to_alist pubs in
    let subs = Hashtbl.to_alist subs in
    Monitor.protect
      (fun () ->
         Deferred.List.iter subs ~how:`Parallel ~f:(fun (_, { r; _ }) -> Reader.close r)
         >>= fun () ->
         Deferred.List.iter pubs ~how:`Parallel ~f:(fun (_, P x) ->
           match x with
           | Concurrent { pub; _ } -> Concurrent.PPub.close pub
           | Exclusive { pub; _ } -> Exclusive.PPub.close pub))
      ~finally:(fun () ->
        (* stop polling *)
        Lo.debug (fun m -> m "closing (freeing) aeron client C structures");
        Aeron.close client;
        Aeron.Context.close ctx;
        Deferred.unit)
;;

exception
  WorkError of
    { err : Err.t
    ; msg : string
    }

let do_work_exn t =
  match main_do_work t.client with
  | -1 -> raise (WorkError { err = errcode (); msg = errmsg () })
  | _ -> ()
;;

let get_error_pipe r =
  let hdr = Bigstring.create 8 in
  let hdrs = Bigsubstring.create hdr in
  let on_w w =
    let rec loop () =
      Reader.really_read_bigsubstring r hdrs
      >>= function
      | `Eof _ -> Deferred.unit
      | `Ok ->
        let errcode = Bigstring.get_int32_be hdr ~pos:0 |> Err.of_int in
        let len = Bigstring.get_int32_be hdr ~pos:4 in
        let bytes = Bytes.create len in
        let strs = Substring.create bytes in
        Reader.really_read_substring r strs
        >>= (function
         | `Eof _ ->
           Pipe.close w;
           Deferred.unit
         | `Ok ->
           let msg = Substring.to_string strs in
           let err =
             Error.create_s [%message "AeronError" ~err:(errcode : Err.t) ~(msg : string)]
           in
           Pipe.write_if_open w (errcode, err) >>= loop)
    in
    loop ()
  in
  Pipe.create_reader ~close_on_exception:false on_w
;;

(* Runs the blocking cnc.dat check off the scheduler thread, the same way
   [create] below runs [init_exn] -- [Aeron.is_driver_active] can block up
   to [timeout_ms] reading a heartbeat file. *)
let is_driver_active ?(timeout_ms = 1000) dir =
  In_thread.run (fun () -> Aeron.is_driver_active dir timeout_ms)
;;

type stalled =
  { pub_connected : bool option
    (** [None] if no connect attempt on this publication has resolved yet
        (still retrying against a client that hasn't reconnected) --
        [is_connected_now] rather than a wait, since anything that waited
        here would just join the same stuck future [offer_bounded] gave
        up on. [Some b] once there is an actual handle to ask. *)
  ; driver_active : bool option
    (** [Some _] only when [offer_bounded] was given [~dir]; [None]
        means "not checked", not "unknown/false". *)
  }
[@@deriving sexp_of]

type offer_outcome =
  | Sent of (int, OfferError.t) result
  | Stalled of stalled
      (** The message was not, and will not be, sent through this attempt.
          Naively racing [offer_pub] against [Clock_ns.with_timeout] is not
          enough to guarantee that: the timeout only stops a caller from
          *waiting*, it does not cancel [offer_pub]'s own bind on
          [connected_or_failed_to_connect], which still fires -- and still
          calls the real [S.offer] -- the moment this publication next
          reconnects, regardless of whether anyone is still waiting on it.
          Confirmed live in test/manual_persistent_driver_test.exe: a
          first, naive version of this delivered a message stalled during
          an outage *after* the reconnect, ahead of one sent for real by a
          later call. [offer_bounded] instead fills an [abandoned] ivar
          the underlying bind checks right before it would call
          [S.offer]/[S.tryclaim], so a [Stalled] attempt really does never
          send. *)
[@@deriving sexp_of]

let default_offer_timeout = Time_ns.Span.of_int_sec 5

(* Bounds [offer_pub_or_abandon] the way [poll_until] bounds add/close: a
   persistent publication's [offer] waits on [connected_or_failed_to_connect],
   which never becomes determined while it's still retrying against a dead
   client/driver, so an unbounded wait here becomes an unbounded wait in
   every caller -- for an actor built on a serial request queue, that stops
   the queue for good (the instrument handler's 21-hour stall,
   ../../../deploy/systemd/README.md, was found in exactly this state).
   Filling [abandoned] on [`Timeout] is what makes this an actual
   cancellation and not just a caller giving up on watching (see [Stalled]
   above). [~dir], when given, spends one extra [is_driver_active] check on
   a stall to say whether the driver itself is the problem, distinct from
   our own client just not having reconnected yet. *)
let offer_bounded_pub ?(timeout = default_offer_timeout) ?dir pub msg =
  let abandoned = Ivar.create () in
  let diagnose () =
    (match dir with
     | None -> Deferred.return None
     | Some dir -> is_driver_active dir >>| Option.some)
    >>| fun driver_active -> Ok (Stalled { pub_connected = is_connected_now pub; driver_active })
  in
  Clock_ns.with_timeout timeout (offer_pub_or_abandon pub ~abandoned msg)
  >>= function
  | `Result (Ok (Some res)) -> Deferred.Or_error.return (Sent res)
  | `Result (Ok None) ->
    (* Only reachable if something else filled [abandoned] before this
       resolved, which nothing does outside the [`Timeout] case below;
       kept as a defensive fallback rather than [assert false]. *)
    diagnose ()
  | `Result (Error _ as err) -> Deferred.return err
  | `Timeout ->
    Ivar.fill_if_empty abandoned ();
    diagnose ()
;;

let offer_bounded t ?timeout ?dir pub msg =
  if Ivar.is_full t.stop then raise Stopped;
  offer_bounded_pub ?timeout ?dir pub msg
;;

(* We set a timeout of one second by default. *)
let create ?driver_timeout dir =
  let nfo = Info.create_s [%message "Aeron_async.create"] in
  Unix.pipe nfo
  >>= fun (`Reader r, `Writer w) ->
  let ba = Bigstring.create 8 in
  Bigstring.set_uint16_be_exn ba ~pos:0 (Fd.to_int_exn w);
  let ctx = Context.create ba in
  Context.set_dir ctx dir;
  Option.iter driver_timeout ~f:(fun x ->
    Context.set_driver_timeout_ms ctx (Time_ns.Span.to_int_ms x));
  Context.set_use_conductor_agent_invoker ctx true;
  (* this might block then return an exn. *)
  Monitor.try_with_or_error (fun () -> In_thread.run (fun () -> init_exn ctx))
  >>|? fun client ->
  let pubs = Int64.Table.create () in
  let stop = Ivar.create () in
  let subs = Int64.Table.create () in
  let t = Fields.create ~ctx ~ba ~client ~pubs ~stop ~subs in
  t, get_error_pipe (Reader.create r)
;;

let is_closed { stop; _ } = Ivar.is_full stop
let close_finished { stop; _ } = Ivar.read stop
let counters_reader { client; _ } = Counters.reader client

let add_concurrent_publication ({ client; pubs; stop; _ } as t) chan ~streamID encode =
  if Ivar.is_full stop then raise Stopped;
  let get_client () = Deferred.Or_error.return (client, close_finished t) in
  let x = Concurrent.create ~get_client chan streamID encode in
  Concurrent.consts x
  >>|? fun consts ->
  Hashtbl.set pubs ~key:consts.registration_id ~data:(P (Concurrent x));
  Concurrent x, consts
;;

let add_exclusive_publication ({ client; pubs; stop; _ } as t) chan ~streamID encode =
  if Ivar.is_full stop then raise Stopped;
  let get_client () = Deferred.Or_error.return (client, close_finished t) in
  let x = Exclusive.create ~get_client chan streamID encode in
  Exclusive.consts x
  >>|? fun consts ->
  Hashtbl.set pubs ~key:consts.registration_id ~data:(P (Exclusive x));
  Exclusive x, consts
;;

let close_subscription_aux { sub; r } =
  Subscription.close sub;
  poll_until ~what:"close subscription" (fun () ->
    Option.some_if (Subscription.is_closed sub) ())
  >>= fun res ->
  (match res with
   | Ok () -> ()
   | Error err -> Lo.err (fun m -> m "%a" Error.pp err));
  (* cleanup regardless: the reader is ours to close. *)
  Reader.close r
;;

let close_subscription t x =
  if Ivar.is_full t.stop then raise Stopped else close_subscription_aux x
;;

let is_connected { sub; _ } = Subscription.is_connected sub

let start_polling_subscription
      ?(stop = Deferred.never ())
      ?(period = Time_ns.Span.of_int_ms 1)
      ?(max_fragments = 10)
      ?(on_fatal = ignore)
      (sub : subscription)
      f
  =
  (* Repeatedly [max_fragments] till [stop] is determined. *)
  let close_sub =
    lazy
      (Lo.debug (fun m -> m "Closing subscription");
       close_subscription_aux sub
       >>| fun () -> Lo.debug (fun m -> m "Closed subscription"))
  in
  (* Launch polling loop. *)
  don't_wait_for
    (let rec loop () =
       if Reader.is_closed sub.r
       then Deferred.unit
       else (
         match Aeron.Subscription.poll_exn sub.sub max_fragments with
         | exception exn ->
           Lo.err (fun m -> m "%s" (Exn.to_string exn));
           (* Let the caller know this subscription is dead beyond this
              point -- e.g. so a [Persistent] subscription notices and
              re-subscribes -- before tearing it down. *)
           on_fatal exn;
           Lazy.force close_sub
         | _nb_frags when Deferred.is_determined stop -> Lazy.force close_sub
         | _ -> Clock_ns.after period >>= loop)
     in
     loop ());
  let bbuf = Bigbuffer.create 4096 in
  let hdr = Bigstring.create Header.sizeof_values in
  let shdr = Bigsubstring.create hdr in
  let buf = Bigstring.create 4096 in
  (* Read data from the C callback via a fd/Reader.t *)
  let rec loop () =
    (* read hdr *)
    Reader.really_read_bigsubstring sub.r shdr
    >>= function
    | `Eof _ ->
      (* TODO: ok? *)
      Deferred.unit
    | `Ok ->
      let h = Header.of_cstruct (Cstruct.of_bigarray hdr) in
      let len = Int32.to_int_exn h.frame.frame_length - 32 in
      (* now read len bytes of payload *)
      Lo.debug (fun m -> m "Read %d bytes from sub" len);
      Reader.really_read_bigsubstring sub.r (Bigsubstring.create buf ~len)
      >>= (function
       | `Eof _ ->
         (* TODO: ok? *)
         Deferred.unit
       | `Ok ->
         (* Lo.debug (fun m -> m "%a" Cstruct.hexdump_pp (Cstruct.of_bigarray buf ~len)); *)
         (match h.frame.flags lsr 6 with
          | 3 ->
            (* unique frame *)
            f (Iobuf.of_bigstring buf ~len);
            loop ()
          | 2 ->
            (* first frame *)
            Bigbuffer.clear bbuf;
            Bigbuffer.add_bigstring bbuf (Bigstring.sub_shared buf ~len);
            loop ()
          | 0 ->
            Bigbuffer.add_bigstring bbuf (Bigstring.sub_shared buf ~len);
            loop ()
          | _ ->
            (* last frame *)
            Bigbuffer.add_bigstring bbuf (Bigstring.sub_shared buf ~len);
            let len = Bigbuffer.length bbuf in
            f (Iobuf.of_bigstring (Bigbuffer.volatile_contents bbuf) ~len);
            loop ()))
  in
  loop ()
;;

let add_subscription ?stop ?period ?max_fragments ?on_fatal t uri ~streamID f =
  if Ivar.is_full t.stop then raise Stopped;
  Unix.pipe (Info.of_string "Aeron_async.add_subscription")
  >>= fun (`Reader rfd, `Writer wfd) ->
  let sub_req = Aeron.Subscription.add t.client uri streamID in
  let wfd_raw = Fd.to_int_exn wfd in
  (* Bounded for the same reason publication's [add] is (see [poll_until]):
     a driver that has stopped answering must not turn this into an
     unbounded wait. *)
  poll_until ~what:"add subscription" (fun () ->
    Aeron.Subscription.add_poll sub_req wfd_raw)
  >>= function
  | Error _ as e -> Fd.close rfd >>= fun () -> Fd.close wfd >>| fun () -> e
  | Ok sub ->
    let consts = Subscription.consts sub in
    let r = Reader.create rfd in
    let sub = Fields_of_subscription.create ~sub ~r in
    Hashtbl.set t.subs ~key:consts.registration_id ~data:sub;
    don't_wait_for
      (start_polling_subscription ?stop ?period ?max_fragments ?on_fatal sub f);
    Deferred.Or_error.return (sub, consts)
;;

(* Auto-reconnecting layer, for producers/consumers that must survive the
   media driver disappearing out from under them -- e.g. across a host
   suspend, which the driver and the client both see as a timeout. When
   [do_work_exn] raises [WorkError], the client's conductor is dead for
   good: nothing in the C client can recover from a driver/client/conductor
   timeout, the only fix is to build a brand new client. [Client] does that
   by wrapping the raw client in a [Persistent_connection_kernel] connection
   -- the same combinator [MkPublication] above already uses for
   publications -- and [add_*_publication] / [add_subscription] below add
   themselves against whichever client is currently connected, so a
   client-level reconnect forces them to re-add against the new one instead
   of retrying forever against a client whose conductor is dead. *)
module Persistent = struct
  (* Captured before [Client] shadows [create] with the persistent-connection
     constructor of the same name. *)
  let raw_create = create

  module Client = struct
    module Conn = struct
      type nonrec t = t

      let close = close
      let is_closed = is_closed
      let close_finished = close_finished
    end

    module M = Persistent_connection_kernel.Make (Conn)
    include M

    module Address = struct
      type t = string [@@deriving sexp_of, equal]
    end

    let create ?driver_timeout ?(do_work_period = Time_ns.Span.of_int_ms 1) dir =
      let connect dir =
        Lo.info (fun m -> m "aeron: connecting client at %s" dir);
        raw_create ?driver_timeout dir
        >>|? fun (conn, errors) ->
        Lo.info (fun m -> m "aeron: client connected");
        don't_wait_for
          (Pipe.iter_without_pushback errors ~f:(fun (errcode, err) ->
             Lo.err (fun m -> m "%a" Error.pp err);
             match errcode with
             | Err.Driver_timeout | Client_timeout | Conductor_service_timeout ->
               (* Confirmed against a real driver: a driver/client/conductor
                  timeout reaches us here, not through [do_work_exn] --
                  the C client's own timeout check sets an internal
                  "terminating" flag and its work loop goes quiet (returns
                  0, not -1) from then on, so [do_work_exn] never raises for
                  this. This error pipe is the only place these show up. *)
               Lo.err (fun m -> m "aeron: client fault, rebuilding client");
               don't_wait_for (Conn.close conn)
             | Client_closed ->
               (* The conductor reported closing directly (see
                  [aeron_on_close_client_t]/[forward_close]) rather than
                  us having to infer it from a timeout code above. Covers
                  both our own [Conn.close] tearing this client down --
                  [Conn.close] is a no-op the second time, since [stop]
                  is already full by then -- and a conductor death this
                  pipe hasn't already reported some other way. *)
               don't_wait_for (Conn.close conn)
             | Buffer_full | Unknown _ -> ()));
        Clock_ns.every' ~stop:(Conn.close_finished conn) do_work_period (fun () ->
          match Result.try_with (fun () -> do_work_exn conn) with
          | Ok () -> Deferred.unit
          | Error exn ->
            Lo.err (fun m -> m "aeron: client fault, rebuilding client: %a" Exn.pp exn);
            Conn.close conn);
        conn
      in
      M.create
        ~server_name:"aeron"
        ~connect
        ~address:(module Address)
        (fun () -> Deferred.Or_error.return dir)
    ;;

    (* The raw client handle publications/subscriptions actually register
       against, once (re)connected, paired with that specific client's own
       death signal (see [MkPublication.create]'s comment on [invalidate]).
       Waits across a reconnect in progress rather than surfacing the
       previous client's death as a caller-visible error, since a fresh add
       is exactly what should happen next. *)
    let raw_client t =
      M.connected_or_failed_to_connect t
      >>|? fun (conn : Conn.t) -> conn.client, Conn.close_finished conn
    ;;

    let counters_reader t =
      raw_client t >>|? fun (client, (_ : unit Deferred.t)) -> Counters.reader client
    ;;
  end

  (* Publications: reuse [Concurrent] / [Exclusive] / [offer_pub] wholesale
     -- a persistent publication is just one whose [get_client] asks a
     [Client.t] on every reconnect attempt instead of returning a fixed
     [Aeron.t]. *)

  let add_concurrent_publication (client : Client.t) chan ~streamID encode =
    let get_client () = Client.raw_client client in
    let x = Concurrent.create ~get_client chan streamID encode in
    Concurrent.consts x >>|? fun consts -> Concurrent x, consts
  ;;

  let add_exclusive_publication (client : Client.t) chan ~streamID encode =
    let get_client () = Client.raw_client client in
    let x = Exclusive.create ~get_client chan streamID encode in
    Exclusive.consts x >>|? fun consts -> Exclusive x, consts
  ;;

  let offer = offer_pub
  let offer_bounded = offer_bounded_pub

  (* Subscriptions: no [S]-shaped module exists for these upstream, so wrap
     them the same way [MkPublication] wraps publications: a small
     [Closable] around [subscription] plus an explicit "this is dead" ivar,
     filled either by [add_subscription]'s poll loop hitting a fatal error
     or by the owning client dying, then handed to
     [Persistent_connection_kernel] for the actual reconnect/backoff. *)
  module Subscription = struct
    module Conn = struct
      type t =
        { sub : subscription
        ; closed : unit Ivar.t
        }

      let close { sub; closed } =
        if Ivar.is_full closed
        then Deferred.unit
        else (
          Ivar.fill_if_empty closed ();
          close_subscription_aux sub)
      ;;

      let is_closed { closed; _ } = Ivar.is_full closed
      let close_finished { closed; _ } = Ivar.read closed
    end

    module M = Persistent_connection_kernel.Make (Conn)
    include M

    module Address = struct
      type t =
        { chan : Uri_sexp.t
        ; stream_id : int32
        }
      [@@deriving sexp, compare, equal]
    end

    let create ?period ?max_fragments (client : Client.t) chan stream_id f =
      M.create
        ~server_name:""
        ~address:(module Address)
        ~connect:(fun { Address.chan; stream_id } ->
          (* [add_subscription] wants the [Aeron_async.t] wrapper (it needs
             [.client], [.stop] and [.subs]), unlike publications which add
             directly against the raw [Aeron.t] handle. *)
          Client.connected_or_failed_to_connect client
          >>=? fun raw ->
          Monitor.try_with_or_error (fun () ->
            let closed = Ivar.create () in
            add_subscription
              ?period
              ?max_fragments
              ~on_fatal:(fun _exn -> Ivar.fill_if_empty closed ())
              raw
              chan
              ~streamID:stream_id
              f
            >>|? fun (sub, _consts) ->
            (* The client dying takes every subscription registered
               through it down with it; notice immediately instead of
               waiting for the next poll tick or a fragment that never
               comes. *)
            don't_wait_for
              (Client.Conn.close_finished raw >>| fun () -> Ivar.fill_if_empty closed ());
            { Conn.sub; closed })
          >>| Or_error.join)
        (fun () -> Deferred.Or_error.return { Address.chan; stream_id })
    ;;

    let consts t =
      M.connected_or_failed_to_connect t
      >>|? fun { Conn.sub; _ } -> Aeron.Subscription.consts sub.sub
    ;;

    let is_connected t =
      M.connected_or_failed_to_connect t
      >>|? fun { Conn.sub; _ } -> is_connected sub
    ;;
  end

  type subscription = Subscription.t

  let add_subscription ?period ?max_fragments client chan ~streamID f =
    let sub = Subscription.create ?period ?max_fragments client chan streamID f in
    Subscription.consts sub >>|? fun consts -> sub, consts
  ;;

  let close_subscription (sub : subscription) = Subscription.close sub
  let is_connected (sub : subscription) = Subscription.is_connected sub
end
