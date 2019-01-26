package kollect.extensions

import arrow.Kind
import arrow.core.Either
import arrow.core.Tuple2
import arrow.extension
import arrow.typeclasses.Monad
import kollect.*
import kollect.Kollect.Unkollect
import kollect.KollectResult.*

// Kollect ops
@extension
interface KollectMonad<F, I> : Monad<KollectPartialOf<F>> {

    fun MF(): Monad<F>

    override fun <A> just(a: A): Kollect<F, A> = Kollect.Unkollect(MF().just(KollectResult.Done(a)))

    override fun <A, B> Kind<KollectPartialOf<F>, A>.map(f: (A) -> B): Kollect<F, B> =
            Kollect.Unkollect(MF().binding {
                val kollect = this@map.fix().run.bind()
                val result = when (kollect) {
                    is Done -> Done<F, B>(f(kollect.x))
                    is Blocked -> Blocked(kollect.rs, kollect.cont.map(f))
                    is Throw -> Throw(kollect.e)
                }
                result
            })

    override fun <A, B> Kind<KollectPartialOf<F>, A>.product(fb: Kind<KollectPartialOf<F>, B>): Kollect<F, Tuple2<A, B>> =
            Unkollect(MF().binding {
                val fab = MF().run { tupled(this@product.fix().run, fb.fix().run).bind() }
                val first = fab.a
                val second = fab.b
                val result = when {
                    first is Throw -> Throw<F, Tuple2<A, B>>(first.e)
                    first is Done && second is Done -> Done(Tuple2(first.x, second.x))
                    first is Done && second is Blocked -> Blocked(second.rs, this@product.product(second.cont))
                    first is Blocked && second is Done -> Blocked(first.rs, first.cont.product(fb))
                    first is Blocked && second is Blocked -> Blocked(combineRequestMaps<I, A, F>(MF(), first.rs, second.rs), first.cont.product(second.cont))
                    // second is Throw
                    else -> Throw((second as Throw).e)
                }
                result
            })

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
