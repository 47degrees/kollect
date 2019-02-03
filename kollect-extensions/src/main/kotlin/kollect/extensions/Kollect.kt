package kollect.extensions

import arrow.Kind
import arrow.core.Either
import arrow.core.Tuple2
import arrow.extension
import arrow.typeclasses.Applicative
import arrow.typeclasses.Functor
import arrow.typeclasses.Monad
import kollect.Kollect
import kollect.KollectPartialOf
import kollect.KollectResult
import kollect.fix

@extension
interface KollectFunctor<F, I> : Functor<KollectPartialOf<F>> {

    fun MF(): Monad<F>

    override fun <A, B> Kind<KollectPartialOf<F>, A>.map(f: (A) -> B): Kollect<F, B> =
            fix().map(MF(), f)
}

@extension
interface KollectApplicative<F, I> : Applicative<KollectPartialOf<F>>, KollectFunctor<F, I> {
    override fun MF(): Monad<F>

    override fun <A> just(a: A): Kollect<F, A> = Kollect.Unkollect(MF().just(KollectResult.Done(a)))

    override fun <A, B> Kind<KollectPartialOf<F>, A>.product(fb: Kind<KollectPartialOf<F>, B>): Kollect<F, Tuple2<A, B>> =
            fix().product<I, B>(MF(), fb)

    override fun <A, B> Kind<KollectPartialOf<F>, A>.map(f: (A) -> B): Kollect<F, B> = fix().map(MF(), f)
}

@extension
interface KollectMonad<F, I> : Monad<KollectPartialOf<F>>, KollectApplicative<F, I> {

    override fun MF(): Monad<F>

    override fun <A, B> tailRecM(a: A, f: (A) -> Kind<KollectPartialOf<F>, Either<A, B>>): Kollect<F, B> =
            Kollect.tailRecM(MF(), a, f)

    override fun <A, B> Kind<KollectPartialOf<F>, A>.map(f: (A) -> B): Kollect<F, B> = fix().map(MF(), f)
    
    override fun <A, B> Kind<KollectPartialOf<F>, A>.flatMap(f: (A) -> Kind<KollectPartialOf<F>, B>): Kollect<F, B> = MF().run {
        fix().flatMap(this, f)
    }
}
