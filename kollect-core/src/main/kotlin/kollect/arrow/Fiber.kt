@file:Suppress("FunctionName")

package arrow.effects

import arrow.Kind
import arrow.core.Either
import arrow.core.Tuple2
import arrow.higherkind
import arrow.instance
import arrow.typeclasses.Applicative
import kollect.arrow.ExitCase

typealias CancelToken<F> = arrow.Kind<F, Unit>

/**
 * Fiber represents the (pure) result of an Async data type (e.g. [[IO]]) being started concurrently and that can be
 * either joined or canceled. You can think of fibers as being lightweight threads, a fiber being a concurrency
 * primitive for doing cooperative multi-tasking.
 */
@higherkind
abstract class Fiber<F, A> : FiberOf<F, A> {

    /**
     * Triggers the cancellation of the fiber.
     *
     * Returns a new task that will trigger the cancellation upon
     * evaluation. Depending on the implementation, this task might
     * await for all registered finalizers to finish, but this behavior
     * is implementation dependent.
     *
     * Note that if the background process that's evaluating the result
     * of the underlying fiber is already complete, then there's nothing
     * to cancel.
     */
    abstract fun cancel(): CancelToken<F>

    /**
     * Returns a new task that will await for the completion of the
     * underlying fiber, (asynchronously) blocking the current run-loop
     * until that result is available.
     */
    abstract fun join(): arrow.Kind<F, A>

    companion object {

        /**
         * Given a `join` and `cancel` tuple, builds a [[Fiber]] value.
         */
        operator fun <F, A> invoke(join: arrow.Kind<F, A>, cancel: CancelToken<F>): Fiber<F, A> =
            Tuple(join, cancel)

        private data class Tuple<F, A>(val join: arrow.Kind<F, A>, val cancel: CancelToken<F>) : Fiber<F, A>() {
            override fun cancel(): CancelToken<F> = cancel
            override fun join(): Kind<F, A> = join
        }
    }
}

@instance(Fiber::class)
interface FiberApplicative<F> : Applicative<FiberPartialOf<F>> {

    fun CF(): Concurrent<F>

    override fun <A> just(a: A): Fiber<F, A> = Fiber(CF().just(a), CF().just(Unit))

    override fun <A, B> Kind<FiberPartialOf<F>, A>.ap(ff: Kind<FiberPartialOf<F>, (A) -> B>): Fiber<F, B> =
        this.map2(ff) { tuple: Tuple2<A, (A) -> B> ->
            tuple.b(tuple.a)
        }

    override fun <A, B, Z> Kind<FiberPartialOf<F>, A>.map2(fb: Kind<FiberPartialOf<F>, B>, f: (Tuple2<A, B>) -> Z): Fiber<F, Z> = CF().run {
        val fa2 = this@map2.fix().join().guaranteeCase {
            when (it) {
                is ExitCase.Failing -> fb.fix().cancel()
                else -> CF().just(Unit)
            }
        }

        val fb2 = fb.fix().join().guaranteeCase {
            when (it) {
                is ExitCase.Failing -> this@map2.fix().cancel()
                else -> CF().just(Unit)
            }
        }

        Fiber(CF().racePair(fa2, fb2).flatMap {
            when (it) {
                is Either.Left -> {
                    val a = it.a.a
                    val fiberB = it.a.b
                    Tuple2(a.just(), fiberB.join()).mapN(CF(), f)
                }
                is Either.Right -> {
                    val fiberA = it.b.a
                    val b = it.b.b
                    Tuple2(fiberA.join(), b.just()).mapN(CF(), f)
                }
            }
        }, this@map2.fix().cancel().map2(fb.fix().cancel()) { Unit })
    }

    override fun <A, B> Kind<FiberPartialOf<F>, A>.product(fb: Kind<FiberPartialOf<F>, B>): Fiber<F, Tuple2<A, B>> =
        this.map2(fb) { it }.fix()

    override fun <A, B> Kind<FiberPartialOf<F>, A>.map(f: (A) -> B): Fiber<F, B> = CF().run {
        Fiber(this@map.fix().join().map(f), this@map.fix().cancel())
    }

    fun unit(): Fiber<F, Unit> = Fiber(CF().just(Unit), CF().just(Unit))
}

fun <F, A0, A1, Z> Tuple2<Kind<F, A0>, Kind<F, A1>>.mapN(AF: Applicative<F>, f: (Tuple2<A0, A1>) -> Z): Kind<F, Z> = AF.run {
    val f0 = this@mapN.a
    val f1 = this@mapN.b
    return f0.product(f1).map { tuple: Tuple2<A0, A1> ->
        f(tuple)
    }
}
