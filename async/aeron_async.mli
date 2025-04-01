open Core
open Async
open Aeron

type t

val create : ?timeout:Time_ns.Span.t -> string -> t
val close : t -> unit Deferred.t

val subscribe
  :  ?stop:_ Deferred.t
  -> t
  -> chan:Uri.t
  -> streamID:int
  -> (Bigstring.t -> Header.t -> unit)
  -> unit Deferred.t

module Publication : sig
  type _ t

  val close : _ t -> unit Deferred.t
  val offer : ?pos:int -> ?len:int -> 'a t -> 'a -> OfferResult.t
  val consts : _ t -> consts
end

val add_publication
  :  t
  -> [< `Concurrent | `Exclusive ]
  -> Uri.t
  -> int
  -> ('a -> Bigstring.t)
  -> [> `Duplicate | `Ok of 'a Publication.t ] Deferred.t

val add_publication_exn
  :  t
  -> [< `Concurrent | `Exclusive ]
  -> Uri.t
  -> int
  -> ('a -> Bigstring.t)
  -> 'a Publication.t Deferred.t
