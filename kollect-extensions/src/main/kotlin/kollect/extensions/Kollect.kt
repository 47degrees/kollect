package kollect.extensions

import arrow.core.Either
import arrow.core.Tuple2
import arrow.extension
import arrow.typeclasses.Applicative
import arrow.typeclasses.Functor
import arrow.typeclasses.Monad
import kollect.*

@extension
interface KollectFunctor<F> : Functor<KollectPartialOf<F>> {

    fun MF(): Monad<F>

    override fun <A, B> KollectOf<F, A>.map(f: (A) -> B): Kollect<F, B> =
            fix().map(MF(), f)
}

@extension
interface KollectApplicative<F, I> : Applicative<KollectPartialOf<F>>, KollectFunctor<F> {
    override fun MF(): Monad<F>

    override fun <A> just(a: A): Kollect<F, A> = Kollect.Unkollect(MF().just(KollectResult.Done(a)))

    override fun <A, B> KollectOf<F, A>.ap(ff: KollectOf<F, (A) -> B>): Kollect<F, B> =
            fix().ap<I, B>(MF(), ff)

    override fun <A, B> KollectOf<F, A>.product(fb: KollectOf<F, B>): Kollect<F, Tuple2<A, B>> =
            fix().product<I, B>(MF(), fb)

    override fun <A, B> KollectOf<F, A>.map(f: (A) -> B): Kollect<F, B> = fix().map(MF(), f)
}

@extension
interface KollectMonad<F, I> : Monad<KollectPartialOf<F>>, KollectApplicative<F, I> {

    override fun MF(): Monad<F>

    override fun <A, B> KollectOf<F, A>.ap(ff: KollectOf<F, (A) -> B>): Kollect<F, B> =
            fix().ap<I, B>(MF(), ff)

    override fun <A, B> tailRecM(a: A, f: (A) -> KollectOf<F, Either<A, B>>): Kollect<F, B> =
            Kollect.tailRecM(MF(), a, f)

    override fun <A, B> KollectOf<F, A>.map(f: (A) -> B): Kollect<F, B> = fix().map(MF(), f)

    override fun <A, B> KollectOf<F, A>.flatMap(f: (A) -> KollectOf<F, B>): Kollect<F, B> = MF().run {
        fix().flatMap(this, f)
    }
}
