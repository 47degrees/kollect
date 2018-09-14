package arrow.effects

import arrow.Kind
import arrow.effects.typeclasses.Async
import arrow.effects.typeclasses.MonadDefer
import arrow.higherkind
import arrow.instance
import java.util.concurrent.atomic.AtomicReference

/**
 * A purely functional synchronization primitive which represents a single value
 * which may not yet be available.
 *
 * When created, a `Deferred` is empty. It can then be completed exactly once,
 * and never be made empty again.
 *
 * `get` on an empty `Deferred` will block until the `Deferred` is completed.
 * `get` on a completed `Deferred` will always immediately return its content.
 *
 * `complete(a)` on an empty `Deferred` will set it to `a`, and notify any and
 * all readers currently blocked on a call to `get`.
 * `complete(a)` on a `Deferred` that has already been completed will not modify
 * its content, and result in a failed `F`.
 *
 * Albeit simple, `Deferred` can be used in conjunction with [[Ref]] to build
 * complex concurrent behaviour and data structures like queues and semaphores.
 *
 * Finally, the blocking mentioned above is semantic only, no actual threads are
 * blocked by the implementation.
 */
abstract class Deferred<F, A> {

    /**
     * Obtains the value of the `Deferred`, or waits until it has been completed.
     * The returned value may be canceled.
     */


    abstract fun get(): arrow.Kind<F, A>

    /**
     * If this `Deferred` is empty, sets the current value to `a`, and notifies
     * any and all readers currently blocked on a `get`.
     *
     * Note that the returned action may complete after the reference
     * has been successfully set: use `F.start(r.complete)` if you want
     * asynchronous behaviour.
     *
     * If this `Deferred` has already been completed, the returned
     * action immediately fails with an `IllegalStateException`. In the
     * uncommon scenario where this behavior is problematic, you can
     * handle failure explicitly using `attempt` or any other
     * `ApplicativeError`/`MonadError` combinator on the returned
     * action.
     *
     * Satisfies:
     *   `Deferred[F, A].flatMap(r => r.complete(a) *> r.get) == a.pure[F]`
     */

    abstract fun complete(a: A): arrow.Kind<F, Unit>

    companion object {


        /** Creates an unset promise. **/

        inline operator fun <F, A> invoke(AF: Async<F>): arrow.Kind<F, Deferred<F, A>> = AF { unsafe<F, A>(AF) }

        /**
         * Like `apply` but returns the newly allocated promise directly
         * instead of wrapping it in `F.delay`.  This method is considered
         * unsafe because it is not referentially transparent -- it
         * allocates mutable state.
         */

        fun <F, A> unsafe(CF: Async<F>): Deferred<F, A> =
            ConcurrentDeferred(CF, AtomicReference(State.Unset<A>(linkedMapOf())))

        /**
         * Creates an unset promise that only requires an [[Async]] and
         * does not support cancellation of `get`.
         *
         * WARN: some `Async` data types, like [[IO]], can be cancelable,
         * making `uncancelable` values unsafe. Such values are only useful
         * for optimization purposes, in cases where the use case does not
         * require cancellation or in cases in which an `F[_]` data type
         * that does not support cancellation is used.
         */

        fun <F, A> uncancelable(MD: Async<F>): arrow.Kind<F, Deferred<F, A>> =
            MD { unsafeUncancelable<Async<F>, F, A>() }

        /**
         * Like [[uncancelable]] but returns the newly allocated promise directly
         * instead of wrapping it in `F.delay`. This method is considered
         * unsafe because it is not referentially transparent -- it
         * allocates mutable state.
         *
         * WARN: read the caveats of [[uncancelable]].
         */
        fun <AF : Async<F>, F, A> unsafeUncancelable(): Deferred<F, A> =
            UncancelabbleDeferred<F, A>(Promise<A>())


        private final class Id

        sealed class State<A> {
            data class Set<A>(val a: A) : State<A>()
            data class Unset<A>(val waiting: LinkedHashMap<Id, (A) -> Unit>) : State<A>()
        }
    }
}

class ConcurrentDeferred<F, A>(val AF: MonadDefer<F>, val ref: AtomicReference<Deferred.Companion.State<A>>) : Deferred<F, A>() {

    override fun get(): Kind<F, A> = AF.defer {
        val refValue = ref.get()
        when (refValue) {
            is Companion.State.Set<A> -> AF.just(refValue.a)
            is Companion.State.Unset<A> -> AF.cancelable {
                cb =>
                val id = unsafeRegister(cb)
                @tailrec
                def unregister (): Unit =
                ref.get match {
                    case State . Set (_) => ()
                    case s @ State.Unset(waiting) =>
                    val updated = State.Unset(waiting - id)
                    if (ref.compareAndSet(s, updated)) ()
                    else unregister()
                }
                F.delay(unregister())
            }
        }
    }

    private[this] def unsafeRegister(cb: Either[ Throwable, A] => Unit): Id = {
    val id = new Id

        @tailrec
        def register (): Option[A] =
    ref.get match {
        case State . Set (a) => Some(a)
        case s @ State.Unset(waiting) =>
        val updated = State.Unset(waiting.updated(id, (a: A) => cb (Right(a))))
        if (ref.compareAndSet(s, updated)) None
        else register()
    }

    register().foreach(a => cb (Right(a)))
    id
}

    def complete (a: A): F[Unit] = {
    def notifyReaders (r: State.Unset[A]): Unit =
    r.waiting.values.foreach {
        cb =>
        cb(a)
    }

    @tailrec
    def loop (): Unit =
    ref.get match {
        case State . Set (_) => throw new IllegalStateException("Attempting to complete a Deferred that has already been completed")
        case s @ State.Unset(_) =>
        if (ref.compareAndSet(s, State.Set(a))) notifyReaders(s)
        else loop()
    }

    F.delay(loop())
}

/*

}

private final class UncancelabbleDeferred[F[_], A](p: Promise[A])(implicit F: Async[F]) extends Deferred[F, A]
{
    def get : F [A] =
        F.async {
            cb =>
            implicit
            val ec: ExecutionContext = TrampolineEC.immediate
            p.future.onComplete {
                case Success (a) => cb(Right(a))
                case Failure (t) => cb(Left(t))
            }
        }

    def complete (a: A): F[Unit] =
    F.delay(p.success(a))
}
}
}*/