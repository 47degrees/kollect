package kollect

import arrow.Kind
import arrow.typeclasses.Functor

fun <F, A> Kind<F, A>.void(FF: Functor<F>): Kind<F, Unit> = FF.run { this@void.`as`(Unit) }
