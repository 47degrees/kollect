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

    companion object {

        /**
         * Derives a [[ContextShift]] instance for `cats.data.EitherT`,
         * given we have one for `F[_]`.
         */

        fun <F, L> deriveEitherT(FF: Functor<F>, cs: ContextShift<F>): ContextShift<EitherTPartialOf<F, L>> =
            object : ContextShift<EitherTPartialOf<F, L>> {
                override fun shift(): Kind<EitherTPartialOf<F, L>, Unit> = FF.run {
                    EitherT(cs.shift().map { Right(it) })
                }

                override fun <A> evalOn(ec: ExecutionContext, fa: Kind<EitherTPartialOf<F, L>, A>): Kind<EitherTPartialOf<F, L>, A> =
                    EitherT(cs.evalOn(ec, fa.value()))
            }

        /**
         * Derives a [[ContextShift]] instance for `cats.data.OptionT`,
         * given we have one for `F[_]`.
         */
        fun <F> deriveOptionT(FF: Functor<F>, cs: ContextShift<F>): ContextShift<OptionTPartialOf<F>> =
            object : ContextShift<OptionTPartialOf<F>> {

                override fun shift(): Kind<OptionTPartialOf<F>, Unit> = FF.run {
                    OptionT(cs.shift().map { Some(it) })
                }

                override fun <A> evalOn(ec: ExecutionContext, fa: Kind<OptionTPartialOf<F>, A>): Kind<OptionTPartialOf<F>, A> =
                    OptionT(cs.evalOn(ec, fa.value()))
            }

        /**
         * Derives a [[ContextShift]] instance for `cats.data.WriterT`,
         * given we have one for `F[_]`.
         */
        fun <F, L> deriveWriterT(AF: Applicative<F>, ML: Monoid<L>, cs: ContextShift<F>): ContextShift<WriterTPartialOf<F, L>> = AF.run {
            object : ContextShift<WriterTPartialOf<F, L>> {
                override fun shift(): Kind<WriterTPartialOf<F, L>, Unit> =
                    WriterT(cs.shift().map { v -> Tuple2(ML.empty(), v) })

                override fun <A> evalOn(ec: ExecutionContext, fa: Kind<WriterTPartialOf<F, L>, A>): Kind<WriterTPartialOf<F, L>, A> =
                    WriterT(cs.evalOn(ec, fa.value()))
            }
        }

        /**
         * Derives a [[ContextShift]] instance for `cats.data.StateT`,
         * given we have one for `F[_]`.
         */
        fun <F, L> deriveStateT(MF: Monad<F>, cs: ContextShift<F>): ContextShift<StateTPartialOf<F, L>> = MF.run {
            object : ContextShift<StateTPartialOf<F, L>> {
                override fun shift(): Kind<StateTPartialOf<F, L>, Unit> =
                    StateT(just { s -> cs.shift().map { a -> Tuple2(s, a) } })

                override fun <A> evalOn(ec: ExecutionContext, fa: Kind<StateTPartialOf<F, L>, A>): Kind<StateTPartialOf<F, L>, A> =
                    StateT.invoke(MF) { s -> cs.evalOn(ec, fa.fix().run(MF, s)) }
            }
        }

        /**
         * Derives a [[ContextShift]] instance for `cats.data.Kleisli`,
         * given we have one for `F[_]`.
         */
        fun <F, R> deriveKleisli(cs: ContextShift<F>): ContextShift<KleisliPartialOf<F, R>> =
            object : ContextShift<KleisliPartialOf<F, R>> {
                override fun shift(): Kind<KleisliPartialOf<F, R>, Unit> =
                    Kleisli.invoke { cs.shift() }

                override fun <A> evalOn(ec: ExecutionContext, fa: Kind<KleisliPartialOf<F, R>, A>): Kind<KleisliPartialOf<F, R>, A> =
                    Kleisli.invoke { a -> cs.evalOn(ec, fa.fix().run(a)) }
            }
    }
}
