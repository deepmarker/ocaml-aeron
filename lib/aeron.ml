open Sexplib.Std
open StdLabels

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
  (* Make a global function callable from C that will be called back
     with a subscription ID. *)

  external create : int -> t = "ml_aeron_fragment_assembler_create"
  external free : t -> unit = "ml_aeron_fragment_assembler_delete" [@@noalloc]

  type cb = Bigstringaf.t -> Header.t -> unit

  let subs = Hashtbl.create 13

  let callback id buf h =
    let subs = Hashtbl.find_all subs id in
    List.iter subs ~f:(fun cb -> cb buf h)
  ;;

  let _ = Callback.register "aeron_frag_asm_cb" callback

  let register_cb id (cb : cb) =
    Hashtbl.add subs id cb;
    create id
  ;;
end

type consts =
  { channel : string
  ; registration_id : int64
  ; stream_id : int32
  ; session_id : int32
  ; channel_status_indicator_id : int32
  }
[@@deriving sexp]

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
  external consts : t -> consts = "ml_aeron_subscription_constants"
  external poll : t -> FragmentAssembler.t -> int -> int = "ml_aeron_subscription_poll"

  let reg_id = ref 0

  let mk_poll f =
    let id = !reg_id in
    incr reg_id;
    let asm = FragmentAssembler.register_cb id f in
    asm, fun t timeout -> poll t asm timeout
  ;;
end

module OfferResult = struct
  type t =
    | NewStreamPosition of int
    | NotConnected
    | BackPressured
    | AdminAction
    | Closed
    | MaxPositionExceeded
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
    | i -> NewStreamPosition i
  ;;
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

module Publication = struct
  type conn = t
  type t
  type add

  external add : conn -> string -> int32 -> add = "ml_aeron_async_add_publication"
  external add_poll : add -> t option = "ml_aeron_async_add_publication_poll"

  let add client uri stream_id = add client (Uri.to_string uri) stream_id

  external close : t -> unit = "ml_aeron_publication_close"
  external is_closed : t -> bool = "ml_aeron_publication_is_closed" [@@noalloc]
  external consts : t -> consts = "ml_aeron_publication_constants"

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
  external consts : t -> consts = "ml_aeron_excl_publication_constants"

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
