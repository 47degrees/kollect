package kollect.arrow.typeclass

import arrow.Kind
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
import arrow.effects.typeclasses.MonadDefer
import arrow.instance
import arrow.typeclasses.Applicative
import arrow.typeclasses.Functor
import arrow.typeclasses.Monad
import arrow.typeclasses.Monoid
import java.util.concurrent.TimeUnit

/**
 * Clock provides the current time, as a pure alternative to:
 *
 *  - Java's
 *    [[https://docs.oracle.com/javase/8/docs/api/java/lang/System.html#currentTimeMillis-- System.currentTimeMillis]]
 *    for getting the "real-time clock" and
 *    [[https://docs.oracle.com/javase/8/docs/api/java/lang/System.html#nanoTime-- System.nanoTime]]
 *    for a monotonic clock useful for time measurements
 *  - JavaScript's `Date.now()` and `performance.now()`
 *
 * `Clock` works with an `F` monadic context that can suspend
 * side effects (e.g. [[IO]]).
 *
 * This is NOT a type class, as it does not have the coherence
 * requirement.
 */
interface Clock<F> {

    /**
     * Returns the current time, as a Unix timestamp (number of time units
     * since the Unix epoch), suspended in `F[_]`.
     *
     * This is the pure equivalent of Java's `System.currentTimeMillis`,
     * or of `CLOCK_REALTIME` from Linux's `clock_gettime()`.
     *
     * The provided `TimeUnit` determines the time unit of the output,
     * its precision, but not necessarily its resolution, which is
     * implementation dependent. For example this will return the number
     * of milliseconds since the epoch:
     *
     * {{{
     *   import scala.concurrent.duration.MILLISECONDS
     *
     *   clock.realTime(MILLISECONDS)
     * }}}
     *
     * N.B. the resolution is limited by the underlying implementation
     * and by the underlying CPU and OS. If the implementation uses
     * `System.currentTimeMillis`, then it can't have a better
     * resolution than 1 millisecond, plus depending on underlying
     * runtime (e.g. Node.js) it might return multiples of 10
     * milliseconds or more.
     *
     * See [[monotonic]], for fetching a monotonic value that
     * may be better suited for doing time measurements.
     */
    fun realTime(unit: TimeUnit): arrow.Kind<F, Long>

    /**
     * Returns a monotonic clock measurement, if supported by the
     * underlying platform.
     *
     * This is the pure equivalent of Java's `System.nanoTime`,
     * or of `CLOCK_MONOTONIC` from Linux's `clock_gettime()`.
     *
     * {{{
     *   clock.monotonic(NANOSECONDS)
     * }}}
     *
     * The returned value can have nanoseconds resolution and represents
     * the number of time units elapsed since some fixed but arbitrary
     * origin time. Usually this is the Unix epoch, but that's not
     * a guarantee, as due to the limits of `Long` this will overflow in
     * the future (2^63^ is about 292 years in nanoseconds) and the
     * implementation reserves the right to change the origin.
     *
     * The return value should not be considered related to wall-clock
     * time, the primary use-case being to take time measurements and
     * compute differences between such values, for example in order to
     * measure the time it took to execute a task.
     *
     * As a matter of implementation detail, the default `Clock[IO]`
     * implementation uses `System.nanoTime` and the JVM will use
     * `CLOCK_MONOTONIC` when available, instead of `CLOCK_REALTIME`
     * (see `clock_gettime()` on Linux) and it is up to the underlying
     * platform to implement it correctly.
     *
     * And be warned, there are platforms that don't have a correct
     * implementation of `CLOCK_MONOTONIC`. For example at the moment of
     * writing there is no standard way for such a clock on top of
     * JavaScript and the situation isn't so clear cut for the JVM
     * either, see:
     *
     *  - [[https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6458294 bug report]]
     *  - [[http://cs.oswego.edu/pipermail/concurrency-interest/2012-January/008793.html concurrency-interest]]
     *    discussion on the X86 tsc register
     *
     * The JVM tries to do the right thing and at worst the resolution
     * and behavior will be that of `System.currentTimeMillis`.
     *
     * The recommendation is to use this monotonic clock when doing
     * measurements of execution time, or if you value monotonically
     * increasing values more than a correspondence to wall-time, or
     * otherwise prefer [[realTime]].
     */
    fun monotonic(unit: TimeUnit): arrow.Kind<F, Long>

    companion object {

        /**
         * Provides Clock instance for any `F` that has `Sync` defined
         */
        fun <F> create(SF: MonadDefer<F>): Clock<F> = object : Clock<F> {
            override fun realTime(unit: TimeUnit): Kind<F, Long> =
                SF { unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS) }

            override fun monotonic(unit: TimeUnit): Kind<F, Long> =
                SF { unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS) }
        }

        /**
         * Default implicit instance — given there's an implicit [[Timer]]
         * in scope, extracts a [[Clock]] instance from it.
         */
        fun <F> extractFromTimer(timer: Timer<F>): Clock<F> = timer.clock()
    }
}

@instance(Clock::class)
interface EitherTClock<F, L> : Clock<EitherTPartialOf<F, L>> {
    fun FF(): Functor<F>

    fun clock(): Clock<F>

    override fun realTime(unit: TimeUnit): EitherT<F, L, Long> = FF().run {
        EitherT(clock().realTime(unit).map { it.right() })
    }

    override fun monotonic(unit: TimeUnit): EitherT<F, L, Long> = FF().run {
        EitherT(clock().monotonic(unit).map { it.right() })
    }
}

@instance(Clock::class)
interface OptionTClock<F> : Clock<OptionTPartialOf<F>> {
    fun FF(): Functor<F>

    fun clock(): Clock<F>

    override fun realTime(unit: TimeUnit): OptionT<F, Long> = FF().run {
        OptionT(clock().realTime(unit).map { it.some() })
    }

    override fun monotonic(unit: TimeUnit): OptionT<F, Long> = FF().run {
        OptionT(clock().monotonic(unit).map { it.some() })
    }
}

@instance(Clock::class)
interface StateTClock<F, S> : Clock<StateTPartialOf<F, S>> {

    fun MF(): Monad<F>

    fun clock(): Clock<F>

    override fun realTime(unit: TimeUnit): StateT<F, S, Long> = StateT.lift(MF(), clock().realTime(unit))

    override fun monotonic(unit: TimeUnit): StateT<F, S, Long> = StateT.lift(MF(), clock().monotonic(unit))
}

@instance(Clock::class)
interface WriterTClock<F, L> : Clock<WriterTPartialOf<F, L>> {

    fun AF(): Applicative<F>

    fun ML(): Monoid<L>

    fun clock(): Clock<F>

    override fun realTime(unit: TimeUnit): WriterT<F, L, Long> = AF().run {
        WriterT(clock().realTime(unit).map { v -> Tuple2(ML().empty(), v) })
    }

    override fun monotonic(unit: TimeUnit): WriterT<F, L, Long> = AF().run {
        WriterT(clock().monotonic(unit).map { v -> Tuple2(ML().empty(), v) })
    }
}

@instance(Clock::class)
interface KleisliClock<F, R> : Clock<KleisliPartialOf<F, R>> {

    fun clock(): Clock<F>

    override fun realTime(unit: TimeUnit): Kind<KleisliPartialOf<F, R>, Long> =
        Kleisli { clock().realTime(unit) }

    override fun monotonic(unit: TimeUnit): Kind<KleisliPartialOf<F, R>, Long> =
        Kleisli { clock().monotonic(unit) }
}
