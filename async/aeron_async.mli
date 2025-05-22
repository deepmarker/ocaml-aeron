open Core
open Async
open Aeron

type t

val create
  :  ?driver_timeout:Time_ns.Span.t
  -> ?idle:Time_ns.Span.t
  -> on_error:(Err.t -> string -> unit)
  -> string
  -> t Deferred.Or_error.t

include Persistent_connection_kernel.Closable with type t := t

(** When the functions below get used with a closed  [t], they will raise an exception. *)

(** Subscription *)

type subscription

val add_subscription
  :  ?stop:_ Deferred.t
  -> ?period:Time_ns.Span.t
  -> ?max_fragments:int
  -> t
  -> Uri.t
  -> streamID:int32
  -> (([< read_write ], 'b) Iobuf.t -> unit)
  -> (subscription * Subscription.consts) Deferred.t

val close_subscription : t -> subscription -> unit Deferred.t

(** Publication *)

type 'a publication

val add_concurrent_publication
  :  t
  -> Uri.t
  -> streamID:int32
  -> ('a -> (read, Iobuf.seek) Iobuf.t)
  -> ('a publication * pub_consts) Deferred.t

val add_exclusive_publication
  :  t
  -> Uri.t
  -> streamID:int32
  -> ('a -> (read, Iobuf.seek) Iobuf.t)
  -> ('a publication * pub_consts) Deferred.t

val offer : t -> 'a publication -> 'a -> OfferResult.t
val close_publication : t -> 'a publication -> unit Deferred.t
