package kollect.arrow

import arrow.Kind
import arrow.core.Right
import arrow.core.Some
import arrow.core.Tuple2
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
import arrow.data.fix
import arrow.data.value
import arrow.instance
import arrow.typeclasses.Applicative
import arrow.typeclasses.Functor
import arrow.typeclasses.Monad
import arrow.typeclasses.Monoid

interface ContextShift<F> {

    /**
     * Asynchronous boundary described as an effectful `F[_]` that
     * can be used in `flatMap` chains to "shift" the continuation
     * of the run-loop to another thread or call stack.
     *
     * This is the [[Async.shift]] operation, without the need for an
     * `ExecutionContext` taken as a parameter.
     *
     */
    fun shift(): arrow.Kind<F, Unit>

    /**
     * Evaluates `f` on the supplied execution context and shifts evaluation
     * back to the default execution environment of `F` at the completion of `f`,
     * regardless of success or failure.
     *
     * The primary use case for this method is executing blocking code on a
     * dedicated execution context.
     *
     * @param ec Execution context where the evaluation has to be scheduled
     * @param fa  Computation to evaluate using `ec`
     */
    fun <A> evalOn(ec: ExecutionContext, fa: arrow.Kind<F, A>): arrow.Kind<F, A>
}

@instance(ContextShift::class)
interface EitherTContextShift<F, L> : ContextShift<EitherTPartialOf<F, L>> {

    fun FF(): Functor<F>

    fun CS(): ContextShift<F>

    override fun shift(): Kind<EitherTPartialOf<F, L>, Unit> = FF().run {
        EitherT(CS().shift().map { Right(it) })
    }

    override fun <A> evalOn(ec: ExecutionContext, fa: Kind<EitherTPartialOf<F, L>, A>): Kind<EitherTPartialOf<F, L>, A> =
        EitherT(CS().evalOn(ec, fa.value()))
}

@instance(ContextShift::class)
interface OptionTContextShift<F> : ContextShift<OptionTPartialOf<F>> {

    fun FF(): Functor<F>

    fun CS(): ContextShift<F>

    override fun shift(): Kind<OptionTPartialOf<F>, Unit> = FF().run {
        OptionT(CS().shift().map { Some(it) })
    }

    override fun <A> evalOn(ec: ExecutionContext, fa: Kind<OptionTPartialOf<F>, A>): Kind<OptionTPartialOf<F>, A> =
        OptionT(CS().evalOn(ec, fa.value()))
}

@instance(ContextShift::class)
interface WriterTContextShift<F, L> : ContextShift<WriterTPartialOf<F, L>> {
    fun AF(): Applicative<F>

    fun ML(): Monoid<L>

    fun CS(): ContextShift<F>

    override fun shift(): Kind<WriterTPartialOf<F, L>, Unit> = AF().run {
        WriterT(CS().shift().map { v -> Tuple2(ML().empty(), v) })
    }

    override fun <A> evalOn(ec: ExecutionContext, fa: Kind<WriterTPartialOf<F, L>, A>): Kind<WriterTPartialOf<F, L>, A> =
        WriterT(CS().evalOn(ec, fa.value()))
}

@instance(ContextShift::class)
interface StateTContextShift<F, L> : ContextShift<StateTPartialOf<F, L>> {

    fun MF(): Monad<F>

    fun CS(): ContextShift<F>

    override fun shift(): Kind<StateTPartialOf<F, L>, Unit> = MF().run {
        StateT(just { s -> CS().shift().map { a -> Tuple2(s, a) } })
    }

    override fun <A> evalOn(ec: ExecutionContext, fa: Kind<StateTPartialOf<F, L>, A>): Kind<StateTPartialOf<F, L>, A> =
        StateT.invoke(MF()) { s -> CS().evalOn(ec, fa.fix().run(MF(), s)) }
}

@instance(ContextShift::class)
interface KleisliContextShift<F, R> : ContextShift<KleisliPartialOf<F, R>> {

    fun CS(): ContextShift<F>

    override fun shift(): Kind<KleisliPartialOf<F, R>, Unit> =
        Kleisli.invoke { CS().shift() }

    override fun <A> evalOn(ec: ExecutionContext, fa: Kind<KleisliPartialOf<F, R>, A>): Kind<KleisliPartialOf<F, R>, A> =
        Kleisli.invoke { a -> CS().evalOn(ec, fa.fix().run(a)) }
}
