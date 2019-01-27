package kollect.test

import arrow.core.Tuple2
import arrow.effects.IO
import arrow.effects.extensions.io.concurrent.concurrent
import arrow.effects.fix
import arrow.effects.typeclasses.Concurrent
import io.kotlintest.runner.junit4.KotlinTestRunner
import io.kotlintest.shouldBe
import io.kotlintest.specs.AbstractStringSpec
import kollect.Kollect
import kollect.extensions.io.timer.timer
import kollect.extensions.kollect.monad.monad
import kollect.fix
import kollect.test.TestHelper.many
import kollect.test.TestHelper.one
import org.junit.runner.RunWith
import kotlin.coroutines.EmptyCoroutineContext

@RunWith(KotlinTestRunner::class)
class KollectTests : AbstractStringSpec() {
    init {
        "We can lift plain values to Fetch" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> =
                    Kollect.just(CF, 42)

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()
            res shouldBe 42
        }

        "We can lift values which have a Data Source to Fetch" {
            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), one(IO.concurrent(), 1))
            val res = io.fix().unsafeRunSync()

            res shouldBe 1
        }

        "We can map over Kollect values" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> =
                    one(CF, 1).map(CF) { it + 1 }

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe 2
        }

        "We can use Kollect inside a for comprehension" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, Int>> =
                    Kollect.monad<F, Int>(CF).binding {
                        val o = one(CF, 1).bind()
                        val t = one(CF, 2).bind()
                        Tuple2(o, t)
                    }.fix()

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe Tuple2(1, 2)
        }

        "We can mix data sources" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, List<Int>>> =
                    Kollect.monad<F, Int>(CF).binding {
                        val o = one(CF, 1).bind()
                        val m = many(CF, 3).bind()
                        Tuple2(o, m)
                    }.fix()

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe Tuple2(1, listOf(0, 1, 2))
        }

        "We can use Kollect as a cartesian" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, List<Int>>> =
                    Kollect.monad<F, Int>(CF).tupled(one(CF, 1), many(CF, 3)).fix()

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe Tuple2(1, listOf(0, 1, 2))
        }
    }
}
