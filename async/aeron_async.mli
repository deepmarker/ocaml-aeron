open Core
open Async
open Aeron

type t

(** [poll_until ~what f] polls [f] on a timer until it yields a value, and
    gives up with an error once [timeout] has passed.

    Aeron's add and close complete asynchronously at the media driver, so
    they have to be polled. A driver that has stopped answering -- it drops
    its clients across a machine suspend -- would otherwise be polled
    forever, which becomes an unbounded wait in every caller and leaves a
    persistent connection with no failed attempt to retry from. Exposed so
    that behaviour can be tested without a live driver. *)
val poll_until
  :  ?timeout:Time_ns.Span.t
  -> what:string
  -> (unit -> 'a option)
  -> 'a Deferred.Or_error.t

(** [create aeron_dir] returns an aeron handle as well as an error
    pipe, that must be processed.  *)
val create
  :  ?driver_timeout:Time_ns.Span.t
  -> string
  -> (t * Error.t Pipe.Reader.t) Deferred.Or_error.t

include Persistent_connection_kernel.Closable with type t := t

exception
  WorkError of
    { err : Err.t
    ; msg : string
    }

(** [do_work_exn t] instructs the Aeron scheduler to perform one unit
    of work. Can raise [WorkError]. Needs to be called as often as you
    want the Aeron scheduler to advance (speed-performance
    tradeoff) *)
val do_work_exn : t -> unit

(** When the functions below get used with a closed [t], they will
    raise an exception. *)

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

module Encoder : sig
  type ('a, 'b) t

  val alloc : ('a -> Bigstring.t) -> ('a, [ `Alloc ]) t
  val direct : ('a -> int) -> (Bigstring.t -> 'a -> unit) -> ('a, [ `Direct ]) t
end

type ('a, 'b) publication

val add_concurrent_publication
  :  t
  -> Uri.t
  -> streamID:int32
  -> ('a, 'b) Encoder.t
  -> (('a, 'b) publication * pub_consts) Deferred.Or_error.t

val add_exclusive_publication
  :  t
  -> Uri.t
  -> streamID:int32
  -> ('a, 'b) Encoder.t
  -> (('a, 'b) publication * pub_consts) Deferred.Or_error.t

val offer
  :  t
  -> ('a, _) publication
  -> 'a
  -> (int, OfferError.t) result Deferred.Or_error.t

val close_publication : (_, _) publication -> unit Deferred.t
