include module type of Aeron_intf

module Err : sig
  type t =
    | Driver_timeout
    | Client_timeout
    | Conductor_service_timeout
    | Buffer_full
    | Client_closed
    | Unknown of int
  [@@deriving sexp]

  val pp : Format.formatter -> t -> unit
  val to_int : t -> int
  val of_int : int -> t
end

module Context : sig
  (** Type of a context. Must NOT be reused between clients! *)
  type t

  val create : Bigstringaf.t -> t
  val close : t -> unit
  val set_dir : t -> string -> unit
  val set_driver_timeout_ms : t -> int -> unit
  val get_driver_timeout_ms : t -> int
  val set_use_conductor_agent_invoker : t -> bool -> unit
  val get_use_conductor_agent_invoker : t -> bool
end

val init_exn : Context.t -> t
val start : t -> unit
val main_do_work : t -> int
val errmsg : unit -> string
val errcode : unit -> Err.t
val close : t -> unit

(** [is_driver_active dirname timeout_ms] checks [dirname] for a live media
    driver's cnc.dat heartbeat without opening a client against it -- e.g.
    to back off a reconnect loop before attempting [init_exn], or for a
    health check that shouldn't need a full client. Blocks up to
    [timeout_ms] only if the heartbeat looks stale. *)
val is_driver_active : string -> int -> bool

(** Version of the linked client library baked in at *its* build time, not
    whatever driver this process happens to be talking to. *)
module Version : sig
  type t =
    { major : int
    ; minor : int
    ; patch : int
    ; text : string (** e.g. ["1.53.0"] *)
    ; full : string (** e.g. ["aeron version=1.53.0 commit=..."] *)
    ; gitsha : string
    }
  [@@deriving sexp]

  val current : unit -> t
  val pp : Format.formatter -> t -> unit
end

(** The counters reader: the same shared-memory counters buffer the media
    driver itself publishes into (publication/subscription positions,
    backpressure, loss, byte/error counts, etc), reachable from a
    connected client. *)
module Counters : sig
  type reader

  val reader : t -> reader
  val max_counter_id : reader -> int32

  (** Dereferences straight into shared memory: every call re-reads
      whatever the driver most recently wrote. *)
  val value : reader -> int32 -> int64

  val label : reader -> int32 -> string
  val type_id : reader -> int32 -> int32

  (** 0 = unused, 1 = allocated, -1 = reclaimed. *)
  val state : reader -> int32 -> int32

  type counter =
    { id : int32
    ; type_id : int32
    ; value : int64
    ; label : string
    }
  [@@deriving sexp]

  (** Every currently-allocated counter -- unused/reclaimed slots are
      skipped, since [type_id]/[label]/[value] on one of those read back
      garbage rather than failing. *)
  val snapshot : reader -> counter list
end

val alloc_claim : unit -> claim
val bigstring_of_claim : claim -> Bigstringaf.t
val commit_claim : claim -> int

module Header : sig
  type t =
    { frame : frame
    ; initial_term_id : int32
    ; position_bits_to_shift : int64
    }

  and frame =
    { frame_length : int32
    ; version : int
    ; flags : int
    ; typ : int
    ; term_offset : int32
    ; session_id : int32
    ; stream_id : int32
    ; term_id : int32
    }
  [@@deriving sexp]

  val sizeof_values : int
  val of_cstruct : Cstruct.t -> t
end

module Subscription : sig
  type conn = t
  type add
  type t

  type consts =
    { registration_id : int64
    ; stream_id : int32
    ; channel_status_indicator_id : int32
    }
  [@@deriving sexp]

  val add : conn -> Uri.t -> int32 -> add

  (** [add_poll add data ~fragment_limit] completes the subscription and
      points it at [data], the buffer a poll deposits fragments into, taking
      at most [fragment_limit] of them each time. C borrows that buffer for
      the life of the subscription, so the caller must keep it reachable. *)
  val add_poll : add -> Bigstringaf.t -> fragment_limit:int -> t option

  (** Also marks [t] {!closing}, before anything else. *)
  val close : t -> unit

  (** Marks [t] {!closing} without touching the C subscription: for one
      whose client is already closed, which freed it. *)
  val mark_closed : t -> unit

  (** [close] or [mark_closed] was called: polls skip [t] from then on. *)
  val closing : t -> bool

  val is_closed : t -> bool
  val is_connected : t -> bool

  (** Weirdly returns -1 for IPC transport. Supposed to return 1 on
      success and -1 on error. *)
  val status : t -> int

  val consts : t -> consts

  (** [poll_exn t] takes up to [fragment_limit] fragments into the buffer
      given to [add_poll] -- each an [aeron_header_values_t] then its
      payload -- and answers how many it took, 0 once [t] is {!closing}. A
      fragment that will not fit is left unconsumed and redelivered by the
      next poll, so a short answer means "drain and call again", never a
      loss.

      Raises if one fragment could not fit an empty buffer, which is
      otherwise a silent livelock; that means the buffer is smaller than
      [aeron.mtu.length]. *)
  val poll_exn : t -> int

  (** Where {!poll_many} reports, two slots per subscription. *)
  type ready = (int, Bigarray.int_elt, Bigarray.c_layout) Bigarray.Array1.t

  (** Room for [n] subscriptions' reports. *)
  val create_ready : int -> ready

  (** [poll_many subs n ready] polls [subs.(0)] to [subs.(n - 1)], as
      [poll_exn] would, in a single call into C, and answers how many of
      them had something to report. Report [k] is the index at
      [ready.{2k}] and the result at [ready.{2k + 1}]: a fragment count
      when positive, {!poll_closed} for a subscription that was skipped,
      and otherwise a failure to hand to {!poll_failure}. Subscriptions
      that took nothing are not reported. [ready] needs room for [n].

      Stops at a failure, leaving the subscriptions after it for the next
      call, so that the failure can still be described. Never allocates:
      the whole point is that polling an idle subscription costs as little
      as Aeron itself makes it. *)
  val poll_many : t array -> int -> ready -> int

  (** The {!poll_many} result of a {!closing} subscription. *)
  val poll_closed : int

  (** What [poll_exn] would have raised for a failed {!poll_many} result.
      Ask straight away: the message behind an Aeron error is only held
      until the next Aeron call. *)
  val poll_failure : t -> int -> exn

  (** Bytes the last poll wrote: how much of the buffer to walk. *)
  val polled_bytes : t -> int
end

module Publication : Publication_sig
module ExclusivePublication : Publication_sig
