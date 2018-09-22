package kollect.arrow.typeclass

import arrow.Kind
import arrow.core.Eval
import arrow.core.FunctionK
import arrow.core.Tuple2
import arrow.typeclasses.Applicative
import arrow.typeclasses.ApplicativeError
import arrow.typeclasses.Monad
import arrow.typeclasses.MonadError
import arrow.typeclasses.Traverse

/**
 * Some types that form a Monad, are also capable of forming an Applicative that supports parallel composition.
 * The Parallel type class allows us to represent this relationship.
 */
interface Parallel<M, F> {
    /**
     * The applicative instance for F
     */
    fun applicative(): Applicative<F>

    /**
     * The monad instance for M
     */
    fun monad(): Monad<M>

    fun apply(): Applicative<F> = applicative()

    fun flatMap(): Monad<M> = monad()

    /**
     * Natural Transformation from the parallel Apply F[_] to the sequential FlatMap M[_].
     */
    fun sequential(): FunctionK<F, M>

    /**
     * Natural Transformation from the sequential FlatMap M[_] to the parallel Apply F[_].
     */
    fun parallel(): FunctionK<M, F>

    /**
     * Provides an `ApplicativeError[F, E]` instance for any F, that has a `Parallel[M, F]`
     * and a `MonadError[M, E]` instance.
     * I.e. if you have a type M[_], that supports parallel composition through type F[_],
     * then you can get `ApplicativeError[F, E]` from `MonadError[M, E]`.
     */
    fun <E> applicativeError(ME: MonadError<M, E>): ApplicativeError<F, E> = object : ApplicativeError<F, E> {

        override fun <A> raiseError(e: E): Kind<F, A> =
            parallel().invoke(ME.raiseError(e))

        override fun <A> Kind<F, A>.handleErrorWith(f: (E) -> Kind<F, A>): Kind<F, A> {
            val ma = ME.run {
                sequential().invoke(this@handleErrorWith).handleErrorWith { sequential().invoke(f(it)) }
            }
            return parallel().invoke(ma)
        }

        override fun <A> just(x: A): Kind<F, A> = applicative().just(x)

        override fun <A, B> Kind<F, A>.ap(ff: Kind<F, (A) -> B>): Kind<F, B> = applicative().run {
            ap(ff)
        }

        override fun <A, B> Kind<F, A>.map(f: (A) -> B): Kind<F, B> = applicative().run {
            map(f)
        }

        override fun <A, B> Kind<F, A>.product(fb: Kind<F, B>): Kind<F, Tuple2<A, B>> = applicative().run {
            product(fb)
        }

        override fun <A, B, Z> Kind<F, A>.map2(fb: Kind<F, B>, f: (Tuple2<A, B>) -> Z): Kind<F, Z> = applicative().run {
            map2(fb, f)
        }

        override fun <A, B, Z> Kind<F, A>.map2Eval(fb: Eval<Kind<F, B>>, f: (Tuple2<A, B>) -> Z): Eval<Kind<F, Z>> =
            applicative().run { map2Eval(fb, f) }
    }

    companion object {

        operator fun <M, F> invoke(P: Parallel<M, F>): Parallel<M, F> = P

        /**
         * Like `Traverse[A].sequence`, but uses the applicative instance
         * corresponding to the Parallel instance instead.
         */
        fun <F, T, M, A> parSequence(P: Parallel<M, F>, TT: Traverse<T>, tma: Kind<T, Kind<M, A>>): Kind<M, Kind<T, A>> {
            val fta: Kind<F, Kind<T, A>> = TT.run { tma.traverse(P.applicative()) { P.parallel().invoke(it) } }
            return P.sequential().invoke(fta)
        }

        /**
         * Like `Traverse[A].traverse`, but uses the applicative instance
         * corresponding to the Parallel instance instead.
         */
        fun <T, M, F, A, B> parTraverse(P: Parallel<M, F>, TT: Traverse<T>, ta: Kind<T, A>, f: (A) -> Kind<M, B>): Kind<M, Kind<T, B>> {
            val gtb: Kind<F, Kind<T, B>> = TT.run { ta.traverse(P.applicative()) { P.parallel().invoke(f(it)) } }
            return P.sequential().invoke(gtb)
        }
    }
}

// TODO Parallel instances are gonna be needed (to provide implementations for sequential() a parallel())