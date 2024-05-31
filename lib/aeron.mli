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

module FragmentAssembler : sig
  type t

  val create : (Bigstringaf.t -> Header.t -> unit) -> t
end

module Subscription : sig
  type conn = t
  type add
  type t

  val add : conn -> Uri.t -> int32 -> add
  val add_poll : add -> t option
  val close : t -> unit
  val is_closed : t -> bool
  val status : t -> int

  type consts =
    { channel : string
    ; registration_id : int64
    ; stream_id : int32
    ; channel_status_indicator_id : int32
    }
  [@@deriving sexp]

  val consts : t -> consts
  val poll : t -> FragmentAssembler.t -> int -> int
end

module Publication : sig
  type conn = t
  type add
  type t

  val add : conn -> Uri.t -> int32 -> add
  val add_poll : add -> t option
  val close : t -> unit
  val is_closed : t -> bool

  type offer_result =
    | Not_connected
    | Back_pressured
    | Admin_action
    | Closed
    | Max_position_exceeded
  [@@deriving sexp]

  val offer : ?pos:int -> ?len:int -> t -> Bigstringaf.t -> (int, offer_result) result
end
