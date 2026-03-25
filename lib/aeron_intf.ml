open Sexplib.Std

type t

type pub_consts =
  { orig_registration_id : int64
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

module OfferError = struct
  type t =
    | NotConnected
    | BackPressured
    | AdminAction
    | Closed
    | MaxPositionExceeded
    | Error
  [@@deriving sexp]
end

type claim

module type Publication_sig = sig
  type conn = t
  type add
  type t

  val add : conn -> Uri.t -> int32 -> add
  val add_poll : add -> t option
  val close : t -> unit
  val is_closed : t -> bool
  val is_connected : t -> bool
  val consts : t -> pub_consts
  val offer : ?pos:int -> ?len:int -> t -> Bigstringaf.t -> (int, OfferError.t) result
  val tryclaim : t -> int -> claim -> (int, OfferError.t) result
end
