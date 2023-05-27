type context
type t
type add_publication
type publication
type add_subscription
type subscription

external create_context : unit -> context = "ml_aeron_context_init"
external close_context : context -> unit = "ml_aeron_context_close"
external context_set_dir : context -> string -> unit = "ml_aeron_context_set_dir"

external context_set_driver_timeout_ms
  :  context
  -> int
  -> unit
  = "ml_aeron_context_set_driver_timeout_ms"

external init : context -> t = "ml_aeron_init"
external start : t -> unit = "ml_aeron_start"
external close : t -> unit = "ml_aeron_close"

external add_publication
  :  t
  -> string
  -> int32
  -> add_publication
  = "ml_aeron_async_add_publication"

let add_publication client uri stream_id =
  add_publication client (Uri.to_string uri) stream_id
;;

external add_subscription
  :  t
  -> string
  -> int32
  -> add_subscription
  = "ml_aeron_async_add_subscription"

let add_subscription client uri stream_id =
  add_subscription client (Uri.to_string uri) stream_id
;;

external close_publication : publication -> unit = "ml_aeron_publication_close"
external close_subscription : subscription -> unit = "ml_aeron_subscription_close"

external publication_is_closed : publication -> bool = "ml_aeron_publication_is_closed"
  [@@noalloc]

external subscription_is_closed : subscription -> bool = "ml_aeron_subscription_is_closed"
  [@@noalloc]

external add_publication_poll
  :  add_publication
  -> publication option
  = "ml_aeron_async_add_publication_poll"

external add_subscription_poll
  :  add_subscription
  -> subscription option
  = "ml_aeron_async_add_subscription_poll"

type offer_result =
  | Not_connected
  | Back_pressured
  | Admin_action
  | Closed
  | Max_position_exceeded
[@@deriving sexp]

external publication_offer
  :  publication
  -> Bigstringaf.t
  -> int
  -> int
  = "ml_aeron_publication_offer"
  [@@noalloc]

let publication_offer pub buf len =
  match publication_offer pub buf len with
  | i when i < 0 -> Result.error (Obj.magic (-i - 1) : offer_result)
  | i -> Ok i
;;

external subscription_status
  :  subscription
  -> int
  = "ml_aeron_subscription_channel_status"
