open Async
open Aeron

val add_publication : t -> Uri.t -> int32 -> publication Deferred.t
val add_subscription : t -> Uri.t -> int32 -> subscription Deferred.t
val close_publication : publication -> unit Deferred.t
val close_subscription : subscription -> unit Deferred.t
