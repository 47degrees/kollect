package kollect.arrow.concurrent

import arrow.Kind
import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.core.Tuple2
import arrow.core.value
import arrow.data.State
import arrow.effects.typeclasses.MonadDefer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * An asynchronous, concurrent mutable reference.
 *
 * Provides safe concurrent access and modification of its content, but no
 * functionality for synchronisation, which is instead handled by Deferred.
 * For this reason, a `Ref` is always initialised to a value.
 *
 * The default implementation is nonblocking and lightweight, consisting essentially
 * of a purely functional wrapper over an `AtomicReference`.
 */
abstract class Ref<F, A> {

    /**
     * Obtains the current value.
     *
     * Since `Ref` is always guaranteed to have a value, the returned action
     * completes immediately after being bound.
     */
    abstract fun get(): arrow.Kind<F, A>

    /**
     * Sets the current value to `a`.
     *
     * The returned action completes after the reference has been successfully set.
     *
     * Satisfies:
     *   `r.set(fa) *> r.get == fa`
     */
    abstract fun set(a: A): arrow.Kind<F, Unit>

    /**
     * Replaces the current value with `a`, returning the previous value.
     */
    abstract fun getAndSet(a: A): arrow.Kind<F, A>

    /**
     * Obtains a snapshot of the current value, and a setter for updating it.
     * The setter may noop (in which case `false` is returned) if another concurrent
     * call to `access` uses its setter first.
     *
     * Once it has noop'd or been used once, a setter never succeeds again.
     *
     * Satisfies:
     *   `r.access.map(_._1) == r.get`
     *   `r.access.flatMap { case (v, setter) => setter(f(v)) } == r.tryUpdate(f).map(_.isDefined)`
     */
    abstract fun access(): arrow.Kind<F, Tuple2<A, (A) -> Kind<F, Boolean>>>

    /**
     * Attempts to modify the current value once, returning `false` if another
     * concurrent modification completes between the time the variable is
     * read and the time it is set.
     */
    abstract fun tryUpdate(f: (A) -> A): arrow.Kind<F, Boolean>

    /**
     * Like `tryUpdate` but allows the update function to return an output value of
     * type `B`. The returned action completes with `None` if the value is not updated
     * successfully and `Some(b)` otherwise.
     */
    abstract fun <B> tryModify(f: (A) -> Tuple2<A, B>): arrow.Kind<F, Option<B>>

    /**
     * Modifies the current value using the supplied update function. If another modification
     * occurs between the time the current value is read and subsequently updated, the modification
     * is retried using the new value. Hence, `f` may be invoked multiple times.
     *
     * Satisfies:
     *   `r.update(_ => a) == r.set(a)`
     */
    abstract fun update(f: (A) -> A): arrow.Kind<F, Unit>

    /**
     * Like `tryModify` but does not complete until the update has been successfully made.
     */
    abstract fun <B> modify(f: (A) -> Tuple2<A, B>): arrow.Kind<F, B>

    /**
     * Update the value of this ref with a state computation.
     *
     * The current value of this ref is used as the initial state and the computed output state
     * is stored in this ref after computation completes. If a concurrent modification occurs,
     * `None` is returned.
     */
    abstract fun <B> tryModifyState(state: State<A, B>): arrow.Kind<F, Option<B>>

    /**
     * Like [[tryModifyState]] but retries the modification until successful.
     */
    abstract fun <B> modifyState(state: State<A, B>): arrow.Kind<F, B>

    companion object {
        /**
         * Builds a `Ref` value for data types that are [[MonadDefer]]
         *
         * This builder uses the
         * [[https://typelevel.org/cats/guidelines.html#partially-applied-type-params Partially-Applied Type]]
         * technique.
         *
         * {{{
         *   Ref<IO>.of(10) <-> Ref.of[IO, Int](10)
         * }}}
         *
         * @see [[of]]
         */
        inline operator fun <F> invoke(MD: MonadDefer<F>): ApplyBuilders<F> = ApplyBuilders(MD)

        /**
         * Creates an asynchronous, concurrent mutable reference initialized to the supplied value.
         *
         * {{{
         *   import cats.effect.IO
         *   import cats.effect.concurrent.Ref
         *
         *   for {
         *     intRef <- Ref.of[IO, Int](10)
         *     ten <- intRef.get
         *   } yield ten
         * }}}
         *
         */
        fun <F, A> of(MD: MonadDefer<F>, a: A): arrow.Kind<F, Ref<F, A>> = MD { unsafe(MD, a) }

        /**
         * Like `apply` but returns the newly allocated ref directly instead of wrapping it in `F.delay`.
         * This method is considered unsafe because it is not referentially transparent -- it allocates
         * mutable state.
         *
         * This method uses the [[http://typelevel.org/cats/guidelines.html#partially-applied-type-params Partially Applied Type Params technique]],
         * so only effect type needs to be specified explicitly.
         *
         * Some care must be taken to preserve referential transparency:
         *
         * {{{
         *   import cats.effect.IO
         *   import cats.effect.concurrent.Ref
         *
         *   class Counter private () {
         *     private val count = Ref.unsafe[IO](0)
         *
         *     def increment: IO[Unit] = count.modify(_ + 1)
         *     def total: IO[Int] = count.get
         *   }
         *
         *   object Counter {
         *     def apply(): IO[Counter] = IO(new Counter)
         *   }
         * }}}
         *
         * Such usage is safe, as long as the class constructor is not accessible and the public one suspends creation in IO
         *
         * The recommended alternative is accepting a `Ref[F, A]` as a parameter:
         *
         * {{{
         *   class Counter (count: Ref[IO, Int]) {
         *     // same body
         *   }
         *
         *   object Counter {
         *     def apply(): IO[Counter] = Ref[IO](0).map(new Counter(_))
         *   }
         * }}}
         */
        fun <F, A> unsafe(MD: MonadDefer<F>, a: A): Ref<F, A> = SyncRef<F, A>(MD, AtomicReference<A>(a))

        class ApplyBuilders<F>(val MD: MonadDefer<F>) : Any() {
            /**
             * Creates an asynchronous, concurrent mutable reference initialized to the supplied value.
             *
             * @see [[Ref.of]]
             */
            fun <A> of(a: A): arrow.Kind<F, Ref<F, A>> = Ref.of(MD, a)
        }

        private class SyncRef<F, A>(val MD: MonadDefer<F>, val ar: AtomicReference<A>) : Ref<F, A>() {

            override fun get(): Kind<F, A> = MD { ar.get() }

            override fun set(a: A): Kind<F, Unit> = MD { ar.set(a) }

            override fun getAndSet(a: A): Kind<F, A> = MD { ar.getAndSet(a) }

            override fun access(): Kind<F, Tuple2<A, (A) -> Kind<F, Boolean>>> = MD {
                val snapshot = ar.get()
                val hasBeenCalled = AtomicBoolean(false)
                val setter = { a: A -> MD { hasBeenCalled.compareAndSet(false, true) && ar.compareAndSet(snapshot, a) } }
                Tuple2(snapshot, setter)
            }

            override fun tryUpdate(f: (A) -> A): Kind<F, Boolean> = MD.run {
                tryModify { a -> Tuple2(f(a), Unit) }.map { it.isDefined() }
            }

            override fun <B> tryModify(f: (A) -> Tuple2<A, B>): arrow.Kind<F, Option<B>> = MD {
                val c = ar.get()
                val (u, b) = f(c)
                if (ar.compareAndSet(c, u)) Some(b)
                else None
            }

            override fun update(f: (A) -> A): Kind<F, Unit> = modify { a -> Tuple2(f(a), Unit) }

            override fun <B> modify(f: (A) -> Tuple2<A, B>): Kind<F, B> {
                tailrec fun spin(): B {
                    val c = ar.get()
                    val (u, b) = f(c)
                    return if (!ar.compareAndSet(c, u)) spin() else b
                }
                return MD { spin() }
            }

            override fun <B> tryModifyState(state: State<A, B>): Kind<F, Option<B>> {
                val f = state.runF.value()
                return tryModify { a -> f(a).value() }
            }

            override fun <B> modifyState(state: State<A, B>): Kind<F, B> {
                val f = state.runF.value()
                return modify { a -> f(a).value() }
            }
        }
    }
}
