@file:Suppress("FunctionName")

package kollect.arrow

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

@instance(Async::class)
interface EitherTAsync<F, L> : Async<EitherTPartialOf<F, L>> {

    fun AF(): Async<F>

    override fun <A> async(fa: Proc<A>): Kind<EitherTPartialOf<F, L>, A> = AF().run {
        EitherT(AF().async(fa).map { it.right() })
    }
}

@instance(Async::class)
interface OptionTAsync<F> : Async<OptionTPartialOf<F>> {

    fun AF(): Async<F>

    override fun <A> async(fa: Proc<A>): Kind<OptionTPartialOf<F>, A> = AF().run {
        OptionT(AF().async(fa).map { it.some() })
    }
}

@instance(Async::class)
interface WriterTAsync<F, L> : Async<WriterTPartialOf<F, L>> {

    fun AF(): Async<F>

    fun ML(): Monoid<L>

    override fun <A> async(fa: Proc<A>): Kind<WriterTPartialOf<F, L>, A> = AF().run {
        WriterT(AF().async(fa).map { v -> Tuple2(ML().empty(), v) })
    }
}

@instance(Async::class)
interface KleisliAsync<F, R> : Async<KleisliPartialOf<F, R>> {

    fun AF(): Async<F>

    override fun <A> async(fa: Proc<A>): Kind<KleisliPartialOf<F, R>, A> =
        Kleisli { AF().async(fa) }
}
