package kollect.arrow

import arrow.Kind
import arrow.core.Either
import arrow.core.Tuple2
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
import arrow.effects.IO
import arrow.instance
import arrow.typeclasses.Applicative
import arrow.typeclasses.Functor
import arrow.typeclasses.Monad
import arrow.typeclasses.Monoid

interface LiftIO<F> {
    fun <A> liftIO(ioa: IO<A>): Kind<F, A>
}

@instance(LiftIO::class)
interface EitherTLiftIO<F, L> : LiftIO<EitherTPartialOf<F, L>> {

    fun LF(): LiftIO<F>

    fun FF(): Functor<F>

    override fun <A> liftIO(ioa: IO<A>): EitherT<F, L, A> =
        FF().run {
            EitherT(LF().liftIO(ioa).map { Either.right(it) })
        }
}

@instance(LiftIO::class)
interface KleisliLiftIO<F, R> : LiftIO<KleisliPartialOf<F, R>> {

    fun LF(): LiftIO<F>

    override fun <A> liftIO(ioa: IO<A>): Kind<KleisliPartialOf<F, R>, A> =
        Kleisli { LF().liftIO(ioa) }
}

@instance(LiftIO::class)
interface OptionTLiftIO<F> : LiftIO<OptionTPartialOf<F>> {

    fun LF(): LiftIO<F>

    fun FF(): Functor<F>

    override fun <A> liftIO(ioa: IO<A>): Kind<OptionTPartialOf<F>, A> = FF().run {
        OptionT(LF().liftIO(ioa).map { it.some() })
    }
}

@instance(LiftIO::class)
interface StateTLiftIO<F, S> : LiftIO<StateTPartialOf<F, S>> {

    fun LF(): LiftIO<F>

    fun MF(): Monad<F>

    override fun <A> liftIO(ioa: IO<A>): Kind<StateTPartialOf<F, S>, A> =
        StateT.lift(MF(), LF().liftIO(ioa))
}

@instance(LiftIO::class)
interface WriterTLiftIO<F, L> : LiftIO<WriterTPartialOf<F, L>> {

    fun LF(): LiftIO<F>

    fun ML(): Monoid<L>

    fun AF(): Applicative<F>

    override fun <A> liftIO(ioa: IO<A>): Kind<WriterTPartialOf<F, L>, A> = AF().run {
        WriterT(LF().liftIO(ioa).map { v -> Tuple2(ML().empty(), v) })
    }
}
