package kollect.test

import arrow.Kind
import arrow.core.None
import arrow.core.Option
import arrow.core.Tuple2
import arrow.core.Tuple5
import arrow.data.NonEmptyList
import arrow.effects.typeclasses.Concurrent
import kollect.DataSource
import kollect.Kollect

object TestHelper {

    object AnException : Throwable()

    data class One(val id: Int)

    object OneSource : DataSource<One, Int> {
        override fun name(): String = "OneSource"

        override fun <F> fetch(CF: Concurrent<F>, id: One): Kind<F, Option<Int>> =
                CF.just(Option(id.id))

        override fun <F> batch(CF: Concurrent<F>, ids: NonEmptyList<One>): Kind<F, Map<One, Int>> =
                CF.just(
                        ids.all.map { v -> Tuple2(v, v.id) }.associateBy({ it.a }, { it.b })
                )
    }

    fun <F> one(CF: Concurrent<F>, id: Int): Kollect<F, Int> =
            Kollect(CF, One(id), OneSource)

    data class Many(val n: Int)

    object ManySource : DataSource<Many, List<Int>> {
        override fun name(): String = "ManySource"

        override fun <F> fetch(CF: Concurrent<F>, id: Many): Kind<F, Option<List<Int>>> =
                CF.just(Option((0 until id.n).toList()))
    }

    fun <F> many(CF: Concurrent<F>, id: Int): Kollect<F, List<Int>> =
            Kollect(CF, Many(id), ManySource)


    data class AnotherOne(val id: Int)

    object AnotheroneSource : DataSource<AnotherOne, Int> {
        override fun name(): String = "AnotherOneSource"

        override fun <F> fetch(CF: Concurrent<F>, id: AnotherOne): Kind<F, Option<Int>> =
                CF.just(Option(id.id))

        override fun <F> batch(CF: Concurrent<F>, ids: NonEmptyList<AnotherOne>): Kind<F, Map<AnotherOne, Int>> =
                CF.just(
                        ids.all.map { v -> Tuple2(v, v.id) }.associateBy({ it.a }, { it.b })
                )
    }

    fun <F> anotherOne(CF: Concurrent<F>, id: Int): Kollect<F, Int> =
            Kollect(CF, AnotherOne(id), AnotheroneSource)

    object Never

    object NeverSource : DataSource<Never, Int> {
        override fun name(): String = "NeverSource"

        override fun <F> fetch(CF: Concurrent<F>, id: Never): Kind<F, Option<Int>> =
                CF.just(None)
    }

    fun <F> never(CF: Concurrent<F>): Kollect<F, Int> =
            Kollect(CF, Never, NeverSource)

    fun <A, B, C, D, E> Tuple5<A, B, C, D, E>.sequence() = listOf(a, b, c, d, e)
}
