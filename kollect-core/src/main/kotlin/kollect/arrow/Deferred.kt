package arrow.effects.deferred

import arrow.Kind
import arrow.concurrent.Promise
import arrow.core.Either
import arrow.core.Failure
import arrow.core.Left
import arrow.core.None
import arrow.core.Option
import arrow.core.Right
import arrow.core.Success
import arrow.core.some
import arrow.effects.Concurrent
import arrow.effects.deferred.Deferred.Companion.Id
import arrow.effects.typeclasses.Async
import arrow.higherkind
import kollect.arrow.ExecutionContext
import kollect.arrow.TrampolineEC
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
@higherkind
abstract class Deferred<F, A> : DeferredOf<F, A> {

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
        operator fun <F, A> invoke(AF: Concurrent<F>): arrow.Kind<F, Deferred<F, A>> = AF { unsafe<F, A>(AF) }

        /**
         * Like `apply` but returns the newly allocated promise directly
         * instead of wrapping it in `F.delay`.  This method is considered
         * unsafe because it is not referentially transparent -- it
         * allocates mutable state.
         */
        fun <F, A> unsafe(CF: Concurrent<F>): Deferred<F, A> =
            ConcurrentDeferred(CF, AtomicReference(State.Unset(linkedMapOf())))

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
            MD.defer { MD.just(unsafeUncancelable<F, A>(MD)) }

        /**
         * Like [[uncancelable]] but returns the newly allocated promise directly
         * instead of wrapping it in `F.delay`. This method is considered
         * unsafe because it is not referentially transparent -- it
         * allocates mutable state.
         *
         * WARN: read the caveats of [[uncancelable]].
         */
        fun <F, A> unsafeUncancelable(AF: Async<F>): Deferred<F, A> =
            UncancelabbleDeferred(AF, Promise())

        class Id

        sealed class State<A> {
            data class Set<A>(val a: A) : State<A>()
            data class Unset<A>(val waiting: LinkedHashMap<Id, (A) -> Unit>) : State<A>()
        }
    }
}

class ConcurrentDeferred<F, A>(val AF: Concurrent<F>, val ref: AtomicReference<Deferred.Companion.State<A>>) : Deferred<F, A>() {

    override fun get(): Kind<F, A> = AF.defer {
        val refValue = ref.get()
        when (refValue) {
            is Companion.State.Set<A> -> AF.just(refValue.a)
            is Companion.State.Unset<A> -> AF.cancelable { cb ->
                val id = unsafeRegister(cb)

                fun unregister(): Unit = ref.get().let { reference ->
                    when (reference) {
                        is Companion.State.Set -> Unit
                        is Companion.State.Unset -> {
                            val waitingClone = reference.waiting.clone() as LinkedHashMap<Id, (A) -> Unit>
                            waitingClone.remove(id)
                            val updated = Companion.State.Unset(waitingClone)
                            if (ref.compareAndSet(reference, updated)) Unit
                            else unregister()
                        }
                    }
                }
                AF.defer { AF.just(unregister()) }
            }
        }
    }

    private fun unsafeRegister(cb: (Either<Throwable, A>) -> Unit): Id {
        val id = Id()

        fun register(): Option<A> = ref.get().let { reference ->
            when (reference) {
                is Companion.State.Set -> reference.a.some()
                is Companion.State.Unset -> {
                    val mapClone = (reference.waiting.clone() as LinkedHashMap<Id, (A) -> Unit>)
                    mapClone[id] = { a: A -> cb(Right(a)) }
                    val updated = Companion.State.Unset(mapClone)
                    if (ref.compareAndSet(reference, updated)) None
                    else register()
                }
            }
        }

        register().fold({ Unit }, { a -> cb(Right(a)) })
        return id
    }

    override fun complete(a: A): Kind<F, Unit> {
        fun notifyReaders(r: Companion.State.Unset<A>): Unit = r.waiting.values.forEach { cb -> cb(a) }

        fun loop(): Unit = ref.get().let { reference ->
            when (reference) {
                is Companion.State.Set -> throw IllegalStateException("Attempting to complete a Deferred that has already been completed")
                is Companion.State.Unset -> if (ref.compareAndSet(reference, Companion.State.Set(a))) notifyReaders(reference)
                else loop()
            }
        }

        return AF.defer { AF.just(loop()) }
    }
}

private class UncancelabbleDeferred<F, A>(val AF: Async<F>, val p: Promise<A>) : Deferred<F, A>() {
    override fun get(): Kind<F, A> = AF.async { cb ->
        val ec: ExecutionContext = TrampolineEC.immediate
        p.future().onComplete(ec) {
            when (it) {
                is Success -> cb(Right(it.value))
                is Failure -> cb(Left(it.exception))
            }
        }
    }

    override fun complete(a: A): Kind<F, Unit> = AF.defer { AF.just(Unit).also { p.success(a) } }
}
