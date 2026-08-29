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
  val add_poll : add -> int -> t option
  val close : t -> unit
  val is_closed : t -> bool
  val is_connected : t -> bool

  (** Weirdly returns -1 for IPC transport. Supposed to return 1 on
      success and -1 on error. *)
  val status : t -> int

  val consts : t -> consts
  val poll_exn : t -> int -> int
end

module Publication : Publication_sig
module ExclusivePublication : Publication_sig
