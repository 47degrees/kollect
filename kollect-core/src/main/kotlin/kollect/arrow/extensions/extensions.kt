package kollect.arrow.extensions

import arrow.Kind
import arrow.core.Tuple2
import arrow.core.toT
import arrow.typeclasses.Functor

fun <F, A, B> Functor<F>.tupleLeft(fa: Kind<F, A>, b: B): Kind<F, Tuple2<B, A>> =
    fa.map { a -> b toT a }
