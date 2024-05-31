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

module FragmentAssembler = struct
  type t

  external create
    :  (Bigstringaf.t -> Header.t -> unit)
    -> t
    = "ml_aeron_fragment_assembler_create"
end

module Subscription = struct
  type conn = t
  type add
  type t

  external add : conn -> string -> int32 -> add = "ml_aeron_async_add_subscription"
  external add_poll : add -> t option = "ml_aeron_async_add_subscription_poll"

  let add client uri stream_id = add client (Uri.to_string uri) stream_id

  external close : t -> unit = "ml_aeron_subscription_close"
  external is_closed : t -> bool = "ml_aeron_subscription_is_closed" [@@noalloc]
  external status : t -> int = "ml_aeron_subscription_channel_status"

  type consts =
    { channel : string
    ; registration_id : int64
    ; stream_id : int32
    ; channel_status_indicator_id : int32
    }
  [@@deriving sexp]

  external consts : t -> consts = "ml_aeron_subscription_constants"
  external poll : t -> FragmentAssembler.t -> int -> int = "ml_aeron_subscription_poll"
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

  external offer : t -> Bigstringaf.t -> int -> int -> int = "ml_aeron_publication_offer"
  [@@noalloc]

  type offer_result =
    | Not_connected
    | Back_pressured
    | Admin_action
    | Closed
    | Max_position_exceeded
  [@@deriving sexp]

  let offer ?(pos = 0) ?len pub buf =
    let buflen = Bigstringaf.length buf in
    let len = Option.value len ~default:buflen in
    if pos < 0 || len < 0 || pos >= len || len > buflen then invalid_arg "offer";
    match offer pub buf pos len with
    | i when i < 0 -> Result.error (Obj.magic (-i - 1) : offer_result)
    | i -> Ok i
  ;;
end
