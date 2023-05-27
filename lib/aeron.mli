type context
type t
type add_publication
type publication
type add_subscription
type subscription

val create_context : unit -> context
val close_context : context -> unit
val context_set_dir : context -> string -> unit
val context_set_driver_timeout_ms : context -> int -> unit
val init : context -> t
val start : t -> unit
val close : t -> unit
val add_publication : t -> Uri.t -> int32 -> add_publication
val add_publication_poll : add_publication -> publication option
val add_subscription : t -> Uri.t -> int32 -> add_subscription
val add_subscription_poll : add_subscription -> subscription option
val close_publication : publication -> unit
val publication_is_closed : publication -> bool
val close_subscription : subscription -> unit
val subscription_is_closed : subscription -> bool
val subscription_status : subscription -> int

type offer_result =
  | Not_connected
  | Back_pressured
  | Admin_action
  | Closed
  | Max_position_exceeded
[@@deriving sexp]

val publication_offer : publication -> Bigstringaf.t -> int -> (int, offer_result) result
