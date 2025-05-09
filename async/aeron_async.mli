open Core
open Async
open Aeron

type t

val create : ?timeout:Time_ns.Span.t -> string -> t
val close : t -> unit Deferred.t

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
end

val poll_subscription
  :  ?stop:_ Deferred.t
  -> ?wait:Time_ns.Span.t
  -> ?max_fragments:int
  -> Subscription.sub
  -> ((read, Iobuf.seek) Iobuf.t -> unit)
  -> unit Deferred.t

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
