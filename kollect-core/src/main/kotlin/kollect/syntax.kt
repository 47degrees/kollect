package kollect

import arrow.effects.typeclasses.Concurrent

object syntax {

    /** syntax to lift any value to the context of Kollect via just */
    class FetchIdSyntax<A>(val a: A) : Any() {

        fun <F> fetch(CF: Concurrent<F>): Kollect<F, A> =
                Kollect.just(CF, a)
    }

    /** syntax to lift exception to Kollect errors */
    class FetchExceptionSyntax<B>(val a: Throwable) : Any() {

        fun <F> fetch(CF: Concurrent<F>): Kollect<F, B> =
                Kollect.error(CF, a)
    }
}