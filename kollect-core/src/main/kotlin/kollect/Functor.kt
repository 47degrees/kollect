package kollect

import arrow.Kind
import arrow.typeclasses.Functor

fun <F, A> Kind<F, A>.void(FF: Functor<F>): Kind<F, Unit> = this.`as`(FF, Unit)

fun <F, A, B> Kind<F, A>.`as`(FF: Functor<F>, b: B): Kind<F, B> = FF.run { map { b } }