package kollect

import arrow.effects.typeclasses.Concurrent

infix fun <F, A> Concurrent<F>.fetch(a: A): Kollect<F, A> = Kollect.just(this, a)

infix fun <F, B> Concurrent<F>.fetch(throwable: Throwable): Kollect<F, B> = Kollect.error(this, throwable)
