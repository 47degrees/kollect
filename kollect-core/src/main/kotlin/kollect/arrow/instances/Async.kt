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
import arrow.data.fix
import arrow.effects.typeclasses.Async
import arrow.effects.typeclasses.Proc
import arrow.instance
import kotlin.coroutines.experimental.CoroutineContext

// Async instances

@instance(EitherT::class)
interface EitherTAsync<F, L> : Async<EitherTPartialOf<F, L>>, EitherTMonadDefer<F, L> {

    override fun MF(): Async<F>

    override fun <A> async(fa: Proc<A>): EitherT<F, L, A> = MF().run {
        EitherT(MF().async(fa).map { it.right() })
    }

    override fun <A> Kind<EitherTPartialOf<F, L>, A>.continueOn(ctx: CoroutineContext): EitherT<F, L, A> = MF().run {
        EitherT(this@continueOn.fix().value.continueOn(ctx))
    }
}

@instance(OptionT::class)
interface OptionTAsync<F> : Async<OptionTPartialOf<F>>, OptionTMonadDefer<F> {

    override fun MF(): Async<F>

    override fun <A> async(fa: Proc<A>): OptionT<F, A> = MF().run {
        OptionT(MF().async(fa).map { it.some() })
    }

    override fun <A> Kind<OptionTPartialOf<F>, A>.continueOn(ctx: CoroutineContext): OptionT<F, A> = MF().run {
        OptionT(this@continueOn.fix().value.continueOn(ctx))
    }
}

@instance(WriterT::class)
interface WriterTAsync<F, L> : Async<WriterTPartialOf<F, L>>, WriterTMonadDefer<F, L> {

    override fun MF(): Async<F>

    override fun <A> async(fa: Proc<A>): WriterT<F, L, A> = MF().run {
        WriterT(MF().async(fa).map { v -> Tuple2(ML().empty(), v) })
    }

    override fun <A> Kind<WriterTPartialOf<F, L>, A>.continueOn(ctx: CoroutineContext): WriterT<F, L, A> = MF().run {
        WriterT(this@continueOn.fix().value.continueOn(ctx))
    }
}

@instance(Kleisli::class)
interface KleisliAsync<F, D> : Async<KleisliPartialOf<F, D>>, KleisliMonadDefer<F, D> {

    override fun MF(): Async<F>

    override fun <A> async(fa: Proc<A>): Kleisli<F, D, A> =
        Kleisli { MF().async(fa) }

    override fun <A> Kind<KleisliPartialOf<F, D>, A>.continueOn(ctx: CoroutineContext): Kleisli<F, D, A> = MF().run {
        Kleisli { d -> this@continueOn.fix().run(d).continueOn(ctx) }
    }
}
