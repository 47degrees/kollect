package kollect.extensions

import arrow.Kind
import arrow.core.Either
import arrow.core.Tuple2
import arrow.extension
import arrow.typeclasses.Monad
import kollect.Kollect
import kollect.Kollect.Unkollect
import kollect.KollectPartialOf
import kollect.KollectResult
import kollect.KollectResult.*
import kollect.fix

// Kollect ops
@extension
interface KollectMonad<F, I> : Monad<KollectPartialOf<F>> {

    fun MF(): Monad<F>

    override fun <A> just(a: A): Kollect<F, A> = Kollect.Unkollect(MF().just(KollectResult.Done(a)))

    override fun <A, B> Kind<KollectPartialOf<F>, A>.map(f: (A) -> B): Kollect<F, B> =
            fix().map(MF(), f)

    override fun <A, B> Kind<KollectPartialOf<F>, A>.product(fb: Kind<KollectPartialOf<F>, B>): Kollect<F, Tuple2<A, B>> =
            fix().product<I, B>(MF(), fb)

    override fun <A, B> tailRecM(a: A, f: (A) -> Kind<KollectPartialOf<F>, Either<A, B>>): Kollect<F, B> =
            f(a).flatMap {
                when (it) {
                    is Either.Left -> tailRecM(a, f)
                    is Either.Right -> just(it.b)
                }
            }.fix()

    override fun <A, B> Kind<KollectPartialOf<F>, A>.flatMap(f: (A) -> Kind<KollectPartialOf<F>, B>): Kollect<F, B> = MF().run {
        Unkollect(this@flatMap.fix().run.flatMap {
            when (it) {
                is Done -> f(it.x).fix().run
                is Throw -> MF().just(Throw(it.e))
                is Blocked -> MF().just(Blocked(it.rs, it.cont.flatMap(f)))
            }
        })
    }
}
