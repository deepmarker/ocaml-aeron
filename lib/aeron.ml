open Sexplib.Std

module Context = struct
  type t

  external create : unit -> t = "ml_aeron_context_init"
  external close : t -> unit = "ml_aeron_context_close"
  external set_dir : t -> string -> unit = "ml_aeron_context_set_dir"

  external set_driver_timeout_ms
    :  t
    -> int
    -> unit
    = "ml_aeron_context_set_driver_timeout_ms"
end

type t

external init : Context.t -> t = "ml_aeron_init"
external start : t -> unit = "ml_aeron_start"
external close : t -> unit = "ml_aeron_close"

module Header = struct
  [%%cstruct
    type values =
      { frame_length : int32_t
      ; version : int8_t
      ; flags : uint8_t
      ; type_ : int16_t
      ; term_offset : int32_t
      ; session_id : int32_t
      ; stream_id : int32_t
      ; term_id : int32_t
      ; reserved_value : int64_t
      ; initial_term_id : int32_t
      ; position_bits_to_shift : uint64_t
      }
    [@@host_endian]]

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

  let of_cstruct cs =
    let frame =
      { frame_length = get_values_frame_length cs
      ; version = get_values_version cs
      ; flags = get_values_flags cs
      ; typ = get_values_type_ cs
      ; term_offset = get_values_term_offset cs
      ; session_id = get_values_session_id cs
      ; stream_id = get_values_stream_id cs
      ; term_id = get_values_term_id cs
      }
    in
    { frame
    ; initial_term_id = get_values_initial_term_id cs
    ; position_bits_to_shift = get_values_position_bits_to_shift cs
    }
  ;;
end

module Subscription = struct
  type consts =
    { registration_id : int64
    ; stream_id : int32
    ; channel_status_indicator_id : int32
    }
  [@@deriving sexp]

  type conn = t
  type add
  type t

  external add : conn -> string -> int32 -> add = "ml_aeron_async_add_subscription"

  external add_poll
    :  add
    -> Bigstringaf.t
    -> Unix.file_descr
    -> t option
    = "ml_aeron_async_add_subscription_poll"

  let add client uri stream_id = add client (Uri.to_string uri) stream_id

  external close : t -> unit = "ml_aeron_subscription_close"
  external is_closed : t -> bool = "ml_aeron_subscription_is_closed" [@@noalloc]
  external status : t -> int = "ml_aeron_subscription_channel_status"
  external consts : t -> consts = "ml_aeron_subscription_constants"
  external poll : t -> int -> int = "ml_aeron_subscription_poll"
end

module OfferResult = struct
  type t =
    | NewStreamPosition of int
    | NotConnected
    | BackPressured
    | AdminAction
    | Closed
    | MaxPositionExceeded
    | Error
  [@@deriving sexp]

  let offer f ?(pos = 0) ?len pub buf =
    let buflen = Bigstringaf.length buf in
    let len = Option.value len ~default:buflen in
    if pos < 0 || len < 0 || pos >= len || len > buflen then invalid_arg "offer";
    match f pub buf pos len with
    | -1 -> NotConnected
    | -2 -> BackPressured
    | -3 -> AdminAction
    | -4 -> Closed
    | -5 -> MaxPositionExceeded
    | -6 -> Error
    | i -> NewStreamPosition i
  ;;
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

module Publication = struct
  type conn = t
  type t
  type add

  external add : conn -> string -> int32 -> add = "ml_aeron_async_add_publication"
  external add_poll : add -> t option = "ml_aeron_async_add_publication_poll"

  let add client uri stream_id = add client (Uri.to_string uri) stream_id

  external close : t -> unit = "ml_aeron_publication_close"
  external is_closed : t -> bool = "ml_aeron_publication_is_closed" [@@noalloc]
  external consts : t -> pub_consts = "ml_aeron_publication_constants"

  external offer : t -> Bigstringaf.t -> int -> int -> int = "ml_aeron_publication_offer"
  [@@noalloc]

  let offer = OfferResult.offer offer
end

module ExclusivePublication = struct
  type conn = t
  type t
  type add

  external add : conn -> string -> int32 -> add = "ml_aeron_async_add_excl_publication"
  external add_poll : add -> t option = "ml_aeron_async_add_excl_publication_poll"

  let add client uri stream_id = add client (Uri.to_string uri) stream_id

  external close : t -> unit = "ml_aeron_excl_publication_close"
  external is_closed : t -> bool = "ml_aeron_excl_publication_is_closed" [@@noalloc]
  external consts : t -> pub_consts = "ml_aeron_excl_publication_constants"

  external offer
    :  t
    -> Bigstringaf.t
    -> int
    -> int
    -> int
    = "ml_aeron_excl_publication_offer"
  [@@noalloc]

  let offer = OfferResult.offer offer
end
