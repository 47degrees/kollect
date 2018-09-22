@file:Suppress("FunctionName")

package kollect.arrow.instances

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
import arrow.data.WriterT
import arrow.data.WriterTPartialOf
import arrow.effects.typeclasses.Async
import arrow.effects.typeclasses.Proc
import arrow.instance
import arrow.typeclasses.Monoid

// Async instances

@instance(EitherT::class)
interface EitherTAsync<F, L> : Async<EitherTPartialOf<F, L>>, EitherTMonadDefer<F, L> {

    override fun MF(): Async<F>

    override fun <A> async(fa: Proc<A>): Kind<EitherTPartialOf<F, L>, A> = MF().run {
        EitherT(MF().async(fa).map { it.right() })
    }
}

@instance(OptionT::class)
interface OptionTAsync<F> : Async<OptionTPartialOf<F>>, OptionTMonadDefer<F> {

    override fun MF(): Async<F>

    override fun <A> async(fa: Proc<A>): Kind<OptionTPartialOf<F>, A> = MF().run {
        OptionT(MF().async(fa).map { it.some() })
    }
}

@instance(WriterT::class)
interface WriterTAsync<F, L> : Async<WriterTPartialOf<F, L>>, WriterTMonadDefer<F, L> {

    override fun MF(): Async<F>

    override fun ML(): Monoid<L>

    override fun <A> async(fa: Proc<A>): Kind<WriterTPartialOf<F, L>, A> = MF().run {
        WriterT(MF().async(fa).map { v -> Tuple2(ML().empty(), v) })
    }
}

@instance(Kleisli::class)
interface KleisliAsync<F, R> : Async<KleisliPartialOf<F, R>>, KleisliMonadDefer<F, R> {

    override fun MF(): Async<F>

    override fun <A> async(fa: Proc<A>): Kind<KleisliPartialOf<F, R>, A> =
        Kleisli { MF().async(fa) }
}
