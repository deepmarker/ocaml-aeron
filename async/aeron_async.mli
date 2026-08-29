open Core
open Async
open Aeron

type t

(** [poll_until ~what f] polls [f] on a timer until it yields a value, and
    gives up with an error once [timeout] has passed.

    Aeron's add and close complete asynchronously at the media driver, so
    they have to be polled. A driver that has stopped answering -- it drops
    its clients across a machine suspend -- would otherwise be polled
    forever, which becomes an unbounded wait in every caller and leaves a
    persistent connection with no failed attempt to retry from. Exposed so
    that behaviour can be tested without a live driver. *)
val poll_until
  :  ?timeout:Time_ns.Span.t
  -> what:string
  -> (unit -> 'a option)
  -> 'a Deferred.Or_error.t

(** [is_driver_active ?timeout_ms dir] checks [dir] for a live media
    driver's cnc.dat heartbeat without opening a client against it -- e.g.
    to back off a reconnect loop before [create], or for a health check
    that shouldn't need a full client. Runs off the scheduler thread since
    the underlying check can block up to [timeout_ms] (default 1s). *)
val is_driver_active : ?timeout_ms:int -> string -> bool Deferred.t

(** [create aeron_dir] returns an aeron handle as well as an error pipe,
    that must be processed. Errors carry the decoded [Err.t] alongside the
    rendered [Error.t] because the two client-facing entry points disagree
    on whether these codes are signed (see the comment on [Err.of_int]):
    [do_work_exn] and this pipe are the only two places a caller can
    distinguish a client-fatal condition (driver/client/conductor timeout)
    from a merely transient one (a full command buffer) without parsing the
    message text. *)
val create
  :  ?driver_timeout:Time_ns.Span.t
  -> string
  -> (t * (Err.t * Error.t) Pipe.Reader.t) Deferred.Or_error.t

include Persistent_connection_kernel.Closable with type t := t

(** [t]'s counters reader -- the same shared-memory buffer the media
    driver publishes positions/backpressure/loss/etc into. See
    [Aeron.Counters]. *)
val counters_reader : t -> Counters.reader

exception
  WorkError of
    { err : Err.t
    ; msg : string
    }

(** [do_work_exn t] instructs the Aeron scheduler to perform one unit
    of work. Can raise [WorkError]. Needs to be called as often as you
    want the Aeron scheduler to advance (speed-performance
    tradeoff) *)
val do_work_exn : t -> unit

(** When the functions below get used with a closed [t], they will
    raise an exception. *)

(** Subscription *)

type subscription

val add_subscription
  :  ?stop:_ Deferred.t
  -> ?period:Time_ns.Span.t
  -> ?max_fragments:int
  -> ?on_fatal:(exn -> unit)
       (** called if the poll loop dies mid-life, before it closes. *)
  -> t
  -> Uri.t
  -> streamID:int32
  -> (([< read_write ], 'b) Iobuf.t -> unit)
  -> (subscription * Subscription.consts) Deferred.Or_error.t

val close_subscription : t -> subscription -> unit Deferred.t
val is_connected : subscription -> bool

(** Publication *)

module Encoder : sig
  type ('a, 'b) t

  val alloc : ('a -> Bigstring.t) -> ('a, [ `Alloc ]) t
  val direct : ('a -> int) -> (Bigstring.t -> 'a -> unit) -> ('a, [ `Direct ]) t
end

type ('a, 'b) publication

val add_concurrent_publication
  :  t
  -> Uri.t
  -> streamID:int32
  -> ('a, 'b) Encoder.t
  -> (('a, 'b) publication * pub_consts) Deferred.Or_error.t

val add_exclusive_publication
  :  t
  -> Uri.t
  -> streamID:int32
  -> ('a, 'b) Encoder.t
  -> (('a, 'b) publication * pub_consts) Deferred.Or_error.t

val offer
  :  t
  -> ('a, _) publication
  -> 'a
  -> (int, OfferError.t) result Deferred.Or_error.t

(** Non-blocking: [None] while the publication's persistent connection has
    no live handle yet (still retrying); [Some b] once it does. Meant for
    diagnosing a stall reported by [offer_bounded], not as a substitute for
    [offer] itself. *)
val is_connected_now : (_, _) publication -> bool option

type stalled =
  { pub_connected : bool option
  ; driver_active : bool option
    (** [Some _] only when [offer_bounded] was given [~dir]; [None] means
        "not checked", not "unknown/false". *)
  }
[@@deriving sexp_of]

type offer_outcome =
  | Sent of (int, OfferError.t) result
  | Stalled of stalled
      (** The message was not, and will not be, sent through this attempt
          -- [offer_bounded] cancels it, rather than merely giving up on
          waiting for it (which would let it silently deliver late, out of
          order, once the publication next reconnects; see the comment in
          the .ml for how that was caught). *)
[@@deriving sexp_of]

(** [offer_bounded t ?timeout ?dir pub msg] is [offer] with the wait
    bounded to [timeout] (default 5s): a persistent publication's [offer]
    waits on its connection attempt resolving, which never happens while
    it's still retrying against a dead client/driver, so an unbounded
    wait here becomes an unbounded wait in every caller. A stall is a
    genuine cancellation, not just a caller giving up (see [Stalled]).
    Pass [~dir] (the publication's media driver directory) to also check
    [is_driver_active] on a stall. *)
val offer_bounded
  :  t
  -> ?timeout:Time_ns.Span.t
  -> ?dir:string
  -> ('a, _) publication
  -> 'a
  -> offer_outcome Deferred.Or_error.t

val close_publication : (_, _) publication -> unit Deferred.t

(** Auto-reconnecting layer. Use this instead of [create] /
    [add_*_publication] / [add_subscription] above for anything long-running
    that must survive the media driver going away and coming back -- e.g.
    across a host suspend. A plain [t] never recovers from that: once
    [do_work_exn] raises [WorkError], the client's conductor is dead for
    good and every publication/subscription added through it is stuck.
    [Persistent.Client.t] rebuilds the whole client when that happens, and
    publications/subscriptions added through it re-add themselves against
    the new one. *)
module Persistent : sig
  module Client : sig
    type t

    (** [create dir] starts connecting immediately and keeps reconnecting,
        with backoff, for as long as [t] is not [close]d -- including every
        time the current client dies. Does not itself fail: callers that
        need a connected client (e.g. before adding a publication) go
        through [add_*_publication] / [add_subscription], which wait. *)
    val create
      :  ?driver_timeout:Time_ns.Span.t
      -> ?do_work_period:Time_ns.Span.t (** default 1ms, as [do_work_exn] asks for. *)
      -> string
      -> t

    include Persistent_connection_kernel.Closable with type t := t

    (** [t]'s counters reader, once connected. See [Aeron.Counters]. *)
    val counters_reader : t -> Counters.reader Deferred.Or_error.t
  end

  (* Publications added here are the same [publication] type as above --
     close them with the plain [close_publication]. *)

  val add_concurrent_publication
    :  Client.t
    -> Uri.t
    -> streamID:int32
    -> ('a, 'b) Encoder.t
    -> (('a, 'b) publication * pub_consts) Deferred.Or_error.t

  val add_exclusive_publication
    :  Client.t
    -> Uri.t
    -> streamID:int32
    -> ('a, 'b) Encoder.t
    -> (('a, 'b) publication * pub_consts) Deferred.Or_error.t

  val offer : ('a, _) publication -> 'a -> (int, OfferError.t) result Deferred.Or_error.t

  (** See [offer_bounded] above -- no owning [t] to check here, since
      these publications aren't tied to a single client. *)
  val offer_bounded
    :  ?timeout:Time_ns.Span.t
    -> ?dir:string
    -> ('a, _) publication
    -> 'a
    -> offer_outcome Deferred.Or_error.t

  type subscription

  val add_subscription
    :  ?period:Time_ns.Span.t
    -> ?max_fragments:int
    -> Client.t
    -> Uri.t
    -> streamID:int32
    -> (([< read_write ], 'b) Iobuf.t -> unit)
    -> (subscription * Subscription.consts) Deferred.Or_error.t

  val close_subscription : subscription -> unit Deferred.t
  val is_connected : subscription -> bool Deferred.Or_error.t
end
