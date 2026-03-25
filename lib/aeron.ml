open Sexplib.Std
include Aeron_intf

let offer_gen f ?(pos = 0) ?len pub buf =
  let buflen = Bigstringaf.length buf in
  let len = Option.value len ~default:buflen in
  if pos < 0 || len < 0 || pos >= len || len > buflen
  then Format.kasprintf invalid_arg "offer: pos=%d len=%d buflen=%d" pos len buflen;
  let open OfferError in
  match f pub buf pos len with
  | -1 -> Result.error NotConnected
  | -2 -> Result.error BackPressured
  | -3 -> Result.error AdminAction
  | -4 -> Result.error Closed
  | -5 -> Result.error MaxPositionExceeded
  | -6 -> Result.error Error
  | i -> Result.ok i
;;

let tryclaim_gen f pub len claim =
  let open OfferError in
  match f pub len claim with
  | -1 -> Result.error NotConnected
  | -2 -> Result.error BackPressured
  | -3 -> Result.error AdminAction
  | -4 -> Result.error Closed
  | -5 -> Result.error MaxPositionExceeded
  | -6 -> Result.error Error
  | i -> Result.ok i
;;

module Err = struct
  type t =
    | Driver_timeout
    | Client_timeout
    | Conductor_service_timeout
    | Buffer_full
    | Unknown of int
  [@@deriving sexp]

  let of_int = function
    | -1000 -> Driver_timeout
    | -1001 -> Client_timeout
    | -1002 -> Conductor_service_timeout
    | -1003 -> Buffer_full
    | i -> Unknown i
  ;;

  let to_int = function
    | Driver_timeout -> -1000
    | Client_timeout -> -1001
    | Conductor_service_timeout -> -1002
    | Buffer_full -> -1003
    | Unknown i -> i
  ;;

  let pp ppf t = Sexplib.Sexp.pp ppf (sexp_of_t t)
end

module Context = struct
  type t

  external create : Bigstringaf.t -> t = "ml_aeron_context_init"
  external close : t -> unit = "ml_aeron_context_close"
  external set_dir : t -> string -> unit = "ml_aeron_context_set_dir"

  external set_driver_timeout_ms
    :  t
    -> int
    -> unit
    = "ml_aeron_context_set_driver_timeout_ms"

  external get_driver_timeout_ms : t -> int = "ml_aeron_context_get_driver_timeout_ms"
  [@@noalloc]

  external set_use_conductor_agent_invoker
    :  t
    -> bool
    -> unit
    = "ml_aeron_context_set_use_conductor_agent_invoker"

  external get_use_conductor_agent_invoker
    :  t
    -> bool
    = "ml_aeron_context_get_use_conductor_agent_invoker"
  [@@noalloc]
end

(* Will block up to driver_timeout_ms if aeronmd is not up (and throw an exn). *)
external init_exn : Context.t -> t = "ml_aeron_init"
external start : t -> unit = "ml_aeron_start"
external main_do_work : t -> int = "ml_aeron_main_do_work" [@@noalloc]
external errmsg : unit -> string = "ml_aeron_errmsg"
external errcode : unit -> int = "ml_aeron_errcode" [@@noalloc]

let errcode () = Err.of_int (errcode ())

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
  type t = Bigstringaf.t (* internal data *)

  external add : conn -> string -> int32 -> add = "ml_aeron_async_add_subscription"

  (* -1 = error, 0 = try again, 1 = success *)
  external add_poll : add -> Bigstringaf.t -> int = "ml_aeron_async_add_subscription_poll"

  let add_poll add fd =
    let buf = Bigstringaf.create (2 * Sys.word_size / 8) in
    (match Sys.word_size, Sys.big_endian with
     | 32, true -> Bigstringaf.set_int32_be buf 0 (Int32.of_int fd)
     | 32, false -> Bigstringaf.set_int32_le buf 0 (Int32.of_int fd)
     | 64, true -> Bigstringaf.set_int64_be buf 0 (Int64.of_int fd)
     | 64, false -> Bigstringaf.set_int64_le buf 0 (Int64.of_int fd)
     | _ -> assert false);
    match add_poll add buf with
    | -1 -> failwith (errmsg ())
    | 0 -> None
    | 1 -> Some buf
    | _ -> assert false
  ;;

  let add client uri stream_id = add client (Uri.to_string uri) stream_id

  external close : Bigstringaf.t -> unit = "ml_aeron_subscription_close"

  external is_closed : Bigstringaf.t -> bool = "ml_aeron_subscription_is_closed"
  [@@noalloc]

  external status : Bigstringaf.t -> int = "ml_aeron_subscription_channel_status"
  external consts : Bigstringaf.t -> consts = "ml_aeron_subscription_constants"
  external poll_exn : Bigstringaf.t -> int -> int = "ml_aeron_subscription_poll"
end

external alloc_claim : unit -> claim = "alloc_claim"
external bigstring_of_claim : claim -> Bigstringaf.t = "bigstring_of_claim"
external commit_claim : claim -> int = "ml_aeron_buffer_claim_commit" [@@noalloc]

module Publication = struct
  type conn = t
  type t
  type add

  external add : conn -> string -> int32 -> add = "ml_aeron_async_add_publication"
  external add_poll : add -> t option = "ml_aeron_async_add_publication_poll"

  let add client uri stream_id = add client (Uri.to_string uri) stream_id

  external close : t -> unit = "ml_aeron_publication_close"
  external is_closed : t -> bool = "ml_aeron_publication_is_closed" [@@noalloc]
  external is_connected : t -> bool = "ml_aeron_publication_is_connected" [@@noalloc]
  external consts : t -> pub_consts = "ml_aeron_publication_constants"

  external offer : t -> Bigstringaf.t -> int -> int -> int = "ml_aeron_publication_offer"
  [@@noalloc]

  external tryclaim : t -> int -> claim -> int = "ml_aeron_publication_try_claim"
  [@@noalloc]

  let offer = offer_gen offer
  let tryclaim = tryclaim_gen tryclaim
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
  external is_connected : t -> bool = "ml_aeron_excl_publication_is_connected" [@@noalloc]
  external consts : t -> pub_consts = "ml_aeron_excl_publication_constants"

  external offer
    :  t
    -> Bigstringaf.t
    -> int
    -> int
    -> int
    = "ml_aeron_excl_publication_offer"
  [@@noalloc]

  external tryclaim : t -> int -> claim -> int = "ml_aeron_excl_publication_try_claim"
  [@@noalloc]

  let offer = offer_gen offer
  let tryclaim = tryclaim_gen tryclaim
end
