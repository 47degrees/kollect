package kollect.extensions

import arrow.core.Tuple2
import arrow.core.right
import arrow.core.some
import arrow.data.*
import arrow.extension
import arrow.typeclasses.Applicative
import arrow.typeclasses.Functor
import arrow.typeclasses.Monoid
import kollect.typeclasses.Clock
import kollect.typeclasses.FiniteDuration
import kollect.typeclasses.Timer

@extension
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

@extension
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

@extension
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

@extension
interface KleisliTimer<F, R> : Timer<KleisliPartialOf<F, R>> {

    fun TF(): Timer<F>

    fun CF(): Clock<F>

    override fun clock(): Clock<KleisliPartialOf<F, R>> = object : KleisliClock<F, R> {
        override fun clock(): Clock<F> = CF()
    }

    override fun sleep(duration: FiniteDuration): Kleisli<F, R, Unit> =
            Kleisli { TF().sleep(duration) }
}
