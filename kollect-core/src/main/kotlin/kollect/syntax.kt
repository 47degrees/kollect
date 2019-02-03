package kollect

import arrow.effects.typeclasses.Concurrent

infix fun <F, A> A.fetch(CF: Concurrent<F>): Kollect<F, A> = Kollect.just(CF, this)

infix fun <F, B> Throwable.fetch(CF: Concurrent<F>): Kollect<F, B> = Kollect.error(CF, this)
