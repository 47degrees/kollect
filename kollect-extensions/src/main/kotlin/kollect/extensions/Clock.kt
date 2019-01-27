package kollect.extensions

import arrow.Kind
import arrow.core.Tuple2
import arrow.core.right
import arrow.core.some
import arrow.data.*
import arrow.effects.ForIO
import arrow.effects.IO
import arrow.extension
import arrow.typeclasses.Applicative
import arrow.typeclasses.Functor
import arrow.typeclasses.Monad
import arrow.typeclasses.Monoid
import kollect.typeclasses.Clock
import kollect.typeclasses.stateT
import java.util.concurrent.TimeUnit

@extension
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

@extension
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

@extension
interface StateTClock<F, S> : Clock<StateTPartialOf<F, S>> {

    fun MF(): Monad<F>

    fun clock(): Clock<F>

    override fun realTime(unit: TimeUnit): StateT<F, S, Long> = stateT(MF(), clock().realTime(unit))

    override fun monotonic(unit: TimeUnit): StateT<F, S, Long> = stateT(MF(), clock().monotonic(unit))
}

@extension
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

@extension
interface KleisliClock<F, R> : Clock<KleisliPartialOf<F, R>> {

    fun clock(): Clock<F>

    override fun realTime(unit: TimeUnit): Kind<KleisliPartialOf<F, R>, Long> =
            Kleisli { clock().realTime(unit) }

    override fun monotonic(unit: TimeUnit): Kind<KleisliPartialOf<F, R>, Long> =
            Kleisli { clock().monotonic(unit) }
}

@extension
interface IOClock : Clock<ForIO> {
    override fun realTime(unit: TimeUnit): IO<Long> =
            IO { unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS) }

    override fun monotonic(unit: TimeUnit): IO<Long> =
            IO { unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS) }
}
