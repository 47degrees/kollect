package kollect.arrow.effects

import arrow.core.Tuple2
import arrow.core.right
import arrow.core.some
import arrow.data.EitherT
import arrow.data.EitherTPartialOf
import arrow.data.Kleisli
import arrow.data.KleisliPartialOf
import arrow.data.OptionT
import arrow.data.OptionTPartialOf
import arrow.data.StateT
import arrow.data.StateTPartialOf
import arrow.data.WriterT
import arrow.data.WriterTPartialOf
import arrow.instance
import arrow.typeclasses.Applicative
import arrow.typeclasses.Functor
import arrow.typeclasses.Monad
import arrow.typeclasses.Monoid
import kollect.arrow.Clock
import kollect.arrow.EitherTClock
import kollect.arrow.KleisliClock
import kollect.arrow.OptionTClock
import kollect.arrow.StateTClock
import kollect.arrow.WriterTClock
import kollect.arrow.concurrent.FiniteDuration

/**
 * Timer is a scheduler of tasks.
 *
 * This is the purely functional equivalent of:
 *
 *  - Java's
 *    [[https://docs.oracle.com/javase/9/docs/api/java/util/concurrent/ScheduledExecutorService.html ScheduledExecutorService]]
 *  - JavaScript's
 *    [[https://developer.mozilla.org/en-US/docs/Web/API/WindowOrWorkerGlobalScope/setTimeout setTimeout]].
 *
 * It provides:
 *
 *  1. the ability to get the current time
 *  1. ability to delay the execution of a task with a specified time duration
 *
 * It does all of that in an `F` monadic context that can suspend
 * side effects and is capable of asynchronous execution (e.g. [[IO]]).
 *
 * This is NOT a type class, as it does not have the coherence
 * requirement.
 */
interface Timer<F> {
    /**
     * Returns a [[Clock]] instance associated with this timer
     * that can provide the current time and do time measurements.
     */
    fun clock(): Clock<F>

    /**
     * Creates a new task that will sleep for the given duration,
     * emitting a tick when that time span is over.
     *
     * As an example on evaluation this will print "Hello!" after
     * 3 seconds:
     *
     * {{{
     *   import cats.effect._
     *   import scala.concurrent.duration._
     *
     *   Timer[IO].sleep(3.seconds).flatMap { _ =>
     *     IO(println("Hello!"))
     *   }
     * }}}
     *
     * Note that `sleep` is required to introduce an asynchronous
     * boundary, even if the provided `timespan` is less or
     * equal to zero.
     */
    fun sleep(duration: FiniteDuration): arrow.Kind<F, Unit>
}

@instance(Timer::class)
interface EitherTTimer<F, L> : Timer<EitherTPartialOf<F, L>> {
    fun FF(): Functor<F>

    fun TF(): Timer<F>

    fun CF(): Clock<F>

    override fun clock(): Clock<EitherTPartialOf<F, L>> = object : EitherTClock<F, L> {
        override fun clock(): Clock<F> = CF()

        override fun FF(): Functor<F> = FF()
    }

    override fun sleep(duration: FiniteDuration): EitherT<F, L, Unit> = FF().run {
        EitherT(TF().sleep(duration).map { it.right() })
    }
}

@instance(Timer::class)
interface OptionTTimer<F> : Timer<OptionTPartialOf<F>> {
    fun FF(): Functor<F>

    fun TF(): Timer<F>

    fun CF(): Clock<F>

    override fun clock(): Clock<OptionTPartialOf<F>> = object : OptionTClock<F> {
        override fun FF(): Functor<F> = FF()

        override fun clock(): Clock<F> = CF()
    }

    override fun sleep(duration: FiniteDuration): OptionT<F, Unit> = FF().run {
        OptionT(TF().sleep(duration).map { it.some() })
    }
}

@instance(Timer::class)
interface WriterTTimer<F, L> : Timer<WriterTPartialOf<F, L>> {

    fun AF(): Applicative<F>

    fun ML(): Monoid<L>

    fun TF(): Timer<F>

    fun CF(): Clock<F>

    override fun clock(): Clock<WriterTPartialOf<F, L>> = object : WriterTClock<F, L> {
        override fun AF(): Applicative<F> = AF()

        override fun ML(): Monoid<L> = ML()

        override fun clock(): Clock<F> = CF()
    }

    override fun sleep(duration: FiniteDuration): WriterT<F, L, Unit> = AF().run {
        WriterT(TF().sleep(duration).map { v -> Tuple2(ML().empty(), v) })
    }
}

@instance(Timer::class)
interface StateTTimer<F, S> : Timer<StateTPartialOf<F, S>> {

    fun MF(): Monad<F>

    fun TF(): Timer<F>

    fun CF(): Clock<F>

    override fun clock(): Clock<StateTPartialOf<F, S>> = object : StateTClock<F, S> {
        override fun MF(): Monad<F> = MF()

        override fun clock(): Clock<F> = CF()
    }

    override fun sleep(duration: FiniteDuration): StateT<F, S, Unit> = StateT.lift(MF(), TF().sleep(duration))
}

@instance(Timer::class)
interface KleisliTimer<F, R> : Timer<KleisliPartialOf<F, R>> {

    fun TF(): Timer<F>

    fun CF(): Clock<F>

    override fun clock(): Clock<KleisliPartialOf<F, R>> = object : KleisliClock<F, R> {
        override fun clock(): Clock<F> = CF()
    }

    override fun sleep(duration: FiniteDuration): Kleisli<F, R, Unit> =
        Kleisli { TF().sleep(duration) }
}
