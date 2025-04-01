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
    ; position_bits_to_shift : int
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
end

type consts =
  { channel : string
  ; registration_id : int64
  ; stream_id : int32
  ; session_id : int32
  ; channel_status_indicator_id : int32
  }
[@@deriving sexp]

module FragmentAssembler : sig
  type t

  val free : t -> unit
end

module Subscription : sig
  type conn = t
  type add
  type t

  val add : conn -> Uri.t -> int32 -> add
  val add_poll : add -> t option
  val close : t -> unit
  val is_closed : t -> bool

  (** Weirdly returns -1 for IPC transport. Supposed to return 1 on
      success and -1 on error. *)
  val status : t -> int

  val consts : t -> consts

  val mk_poll
    :  (Bigstringaf.t -> Header.t -> unit)
    -> FragmentAssembler.t * (t -> int -> int)
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

module type Publication_sig = sig
  type conn = t
  type add
  type t

  val add : conn -> Uri.t -> int32 -> add
  val add_poll : add -> t option
  val close : t -> unit
  val is_closed : t -> bool
  val consts : t -> consts
  val offer : ?pos:int -> ?len:int -> t -> Bigstringaf.t -> OfferResult.t
end

module Publication : Publication_sig
module ExclusivePublication : Publication_sig
