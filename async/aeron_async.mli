open Core
open Async
open Aeron

module Publication : sig
  val add : t -> Uri.t -> int32 -> Publication.t Deferred.t
  val close : Publication.t -> unit Deferred.t
end

module Subscription : sig
  val add : t -> Uri.t -> int32 -> Subscription.t Deferred.t
  val close : Subscription.t -> unit Deferred.t

  val poll
    :  ?stop:unit Deferred.t
    -> Subscription.t
    -> (Bigstring.t -> Header.t -> unit)
    -> unit Deferred.t
end
