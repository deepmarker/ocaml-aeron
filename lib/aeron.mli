module Context : sig
  type t

  val create : unit -> t
  val close : t -> unit
  val set_dir : t -> string -> unit
  val set_driver_timeout_ms : t -> int -> unit
end

type t

val init : Context.t -> t
val start : t -> unit
val close : t -> unit

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
  val add_poll : add -> Unix.file_descr -> t option
  val close : t -> unit
  val is_closed : t -> bool

  (** Weirdly returns -1 for IPC transport. Supposed to return 1 on
      success and -1 on error. *)
  val status : t -> int

  val consts : t -> consts
  val poll_exn : t -> int -> int
end

module OfferResult : sig
  type t =
    | NewStreamPosition of int
    | NotConnected
    | BackPressured
    | AdminAction
    | Closed
    | MaxPositionExceeded
    | Error
  [@@deriving sexp]
end

type pub_consts =
  { orig_registrsation_id : int64
  ; registration_id : int64
  ; max_possible_position : int64
  ; position_bits_to_shift : int64
  ; term_buffer_length : int64
  ; max_message_length : int64
  ; max_payload_length : int64
  ; stream_id : int32
  ; session_id : int32
  ; initial_term_id : int32
  ; publication_limit_counter_id : int32
  ; channel_status_indicator_id : int32
  }
[@@deriving sexp]

module type Publication_sig = sig
  type conn = t
  type add
  type t

  val add : conn -> Uri.t -> int32 -> add
  val add_poll : add -> t option
  val close : t -> unit
  val is_closed : t -> bool
  val consts : t -> pub_consts
  val offer : ?pos:int -> ?len:int -> t -> Bigstringaf.t -> OfferResult.t
end

module Publication : Publication_sig
module ExclusivePublication : Publication_sig
