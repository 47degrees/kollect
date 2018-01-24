package kollect.arrow.extensions

import arrow.*
import arrow.core.*
import arrow.typeclasses.*

fun <F, A, B> Functor<F>.tupleLeft(fa: HK<F, A>, b: B): HK<F, Tuple2<B, A>> =
        map(fa, { a -> b toT a })

inline fun <reified F, A, B> HK<F, A>.tupleLeft(b: B, FF: Functor<F> = functor()): HK<F, Tuple2<B, A>> =
        FF.tupleLeft(this, b)