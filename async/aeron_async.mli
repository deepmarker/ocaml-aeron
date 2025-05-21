open Core
open Async
open Aeron

type t

include Persistent_connection_kernel.Closable with type t := t

val create
  :  ?driver_timeout:Time_ns.Span.t
  -> ?idle:Time_ns.Span.t
  -> on_error:(Err.t -> string -> unit)
  -> string
  -> t Deferred.Or_error.t

module Publication : sig
  type _ t

  val close : _ t -> unit Deferred.t
  val offer : 'a t -> 'a -> OfferResult.t
  val consts : _ t -> pub_consts
end

module Subscription : sig
  type sub

  val create : t -> Uri.t -> int -> sub Deferred.t
  val close : sub -> unit Deferred.t

  val start_polling_loop
    :  ?stop:_ Deferred.t
    -> ?wait:Time_ns.Span.t
    -> ?max_fragments:int
    -> sub
    -> ((read, Iobuf.seek) Iobuf.t -> unit)
    -> unit Deferred.t
end

val add_publication
  :  t
  -> [< `Concurrent | `Exclusive ]
  -> Uri.t
  -> int
  -> ('a -> (read, Iobuf.seek) Iobuf.t)
  -> [> `Duplicate | `Ok of 'a Publication.t ] Deferred.t

val add_publication_exn
  :  t
  -> [< `Concurrent | `Exclusive ]
  -> Uri.t
  -> int
  -> ('a -> (read, Iobuf.seek) Iobuf.t)
  -> 'a Publication.t Deferred.t
