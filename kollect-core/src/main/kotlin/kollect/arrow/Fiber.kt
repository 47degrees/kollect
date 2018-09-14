package arrow.effects

import arrow.Kind
import arrow.higherkind
import arrow.instance
import arrow.typeclasses.Applicative

typealias CancelToken<F> = arrow.Kind<F, Unit>


/**
 * `Fiber` represents the (pure) result of an [[Async]] data type (e.g. [[IO]]) being started concurrently and that can
 * be either joined or canceled. You can think of fibers as being lightweight threads, a fiber being a concurrency
 * primitive for doing cooperative multi-tasking.
 *
 * For example a `Fiber` value is the result of evaluating [[IO.start]]:
 *
 * {{{
 *   val io = IO.shift *> IO(println("Hello!"))
 *
 *   val fiber: IO[Fiber[IO, Unit]] = io.start
 * }}}
 *
 * Usage example:
 *
 * {{{
 *   for {
 *     fiber <- IO.shift *> launchMissiles.start
 *     _ <- runToBunker.handleErrorWith { error =>
 *       // Retreat failed, cancel launch (maybe we should
 *       // have retreated to our bunker before the launch?)
 *       fiber.cancel *> IO.raiseError(error)
 *     }
 *     aftermath <- fiber.join
 *   } yield {
 *     aftermath
 *   }
 * }}}
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
        fun <F, A> just(join: arrow.Kind<F, A>, cancel: CancelToken<F>): Fiber<F, A> =
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

    override fun <A> just(a: A): Kind<FiberPartialOf<F>, A> = Fiber(CF.just(a), CF.unit())
}
/*
private abstract class FiberInstances {

    fun <F> fiberApplicative(CF: Concurrent<F>): Applicative<FiberKindPartial<F>> = object : Applicative<FiberKindPartial<F>> {
        final override def pure [A](x: A): Fiber[F, A] =
        Fiber(F.pure(x), F.unit)

        final override def ap [A, B](ff: Fiber[ F, A => B])(fa: Fiber[F, A]): Fiber[F, B] =
        map2(ff, fa)(_(_))
        final override def map2 [A, B, Z](fa: Fiber[ F, A], fb: Fiber[F, B])(f: (A, B) => Z): Fiber[F, Z] =
        {
            val fa2 = F.guaranteeCase(fa.join) { case ExitCase . Error (_) => fb.cancel; case _ => F . unit }
            val fb2 = F.guaranteeCase(fb.join) { case ExitCase . Error (_) => fa.cancel; case _ => F . unit }
            Fiber(
                F.racePair(fa2, fb2).flatMap {
                    case Left ((a, fiberB)) => (a.pure[F], fiberB.join).mapN(f)
                    case Right ((fiberA, b)) => (fiberA.join, b.pure[F]).mapN(f)
                },
                F.map2(fa.cancel, fb.cancel)((_, _) =>()))
        }
        final override def product [A, B](fa: Fiber[ F, A], fb: Fiber[F, B]): Fiber[F, (A, B)] =
        map2(fa, fb)((_, _))
        final override def map [A, B](fa: Fiber[ F, A])(f: A => B): Fiber[F, B] =
        Fiber(F.map(fa.join)(f), fa.cancel)
        final override val unit: Fiber[F, Unit] =
        Fiber(F.unit, F.unit)
    }

    implicit def fiberMonoid[F[_]: Concurrent, M[_], A: Monoid]: Monoid[Fiber[F, A]] =
    Applicative.monoid[Fiber[F, ?], A]
}

*/

