open Core
open Async
open Aeron

type endpoint =
  { dir : string
  ; channel : Uri.t
  ; stream : int32
  ; timeout : Time_ns.Span.t option
  }
[@@deriving fields]

type 'a t =
  { ctx : Context.t
  ; chn : Uri.t
  ; client : Aeron.t
  ; v : 'a
  }
[@@deriving fields]

module type Publication_sig = sig
  type pub

  val add : Aeron.t -> Uri.t -> int32 -> pub Deferred.t
  val close : pub -> unit Deferred.t

  module EZ : sig
    val create : ?timeout:Time_ns.Span.t -> string -> Uri.t -> int32 -> pub t Deferred.t
    val create_endpoint : endpoint -> pub t Deferred.t
    val close : pub t -> unit Deferred.t
    val offer : ?pos:int -> ?len:int -> pub t -> Bigstring.t -> OfferResult.t
  end
end

module Publication : Publication_sig with type pub := Publication.t
module ExclusivePublication : Publication_sig with type pub := ExclusivePublication.t

module Subscription : sig
  val add : Aeron.t -> Uri.t -> int32 -> Subscription.t Deferred.t
  val close : Subscription.t -> unit Deferred.t

  val poll
    :  ?stop:_ Deferred.t
    -> Subscription.t
    -> (Bigstring.t -> Header.t -> unit)
    -> unit Deferred.t

  val subscribe_direct
    :  ?stop:_ Deferred.t
    -> Aeron.t
    -> chan:Uri.t
    -> streamID:int
    -> (Bigstringaf.t -> Header.t -> unit)
    -> unit Deferred.t

  module EZ : sig
    val create_endpoint : endpoint -> Subscription.t t Deferred.t
    val close : Subscription.t t -> unit Deferred.t

    val create
      :  ?timeout:Time_ns.Span.t
      -> string
      -> Uri.t
      -> int32
      -> Subscription.t t Deferred.t

    val poll
      :  ?stop:_ Deferred.t
      -> Subscription.t t
      -> (Bigstring.t -> Header.t -> unit)
      -> unit Deferred.t
  end
end
