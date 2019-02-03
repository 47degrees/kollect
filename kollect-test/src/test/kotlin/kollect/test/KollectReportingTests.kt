package kollect.test

import arrow.Kind
import arrow.core.Tuple2
import arrow.data.ForListK
import arrow.data.extensions.list.traverse.traverse
import arrow.effects.IO
import arrow.effects.extensions.io.concurrent.concurrent
import arrow.effects.extensions.io.functor.map
import arrow.effects.typeclasses.Concurrent
import io.kotlintest.runner.junit4.KotlinTestRunner
import io.kotlintest.shouldBe
import kollect.Kollect
import kollect.extensions.io.timer.timer
import kollect.extensions.kollect.monad.monad
import kollect.fix
import kollect.test.TestHelper.many
import kollect.test.TestHelper.one
import org.junit.runner.RunWith
import kotlin.coroutines.EmptyCoroutineContext

@RunWith(KotlinTestRunner::class)
class KollectReportingTests : KollectSpec() {

    init {

        "Plain values have no rounds of execution" {
            fun <F> kollect(CF: Concurrent<F>) = Kollect.just(CF, 42)

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.a.rounds.size shouldBe 0
            }.unsafeRunSync()
        }

        "Single kollects are executed in one round" {
            fun <F> kollect(CF: Concurrent<F>) = one(CF, 1)

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.a.rounds.size shouldBe 1
            }.unsafeRunSync()
        }

        "Single kollects are executed in one round per binding in a for comprehension" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val o = one(CF, 1).bind()
                    val t = one(CF, 2).bind()
                    Tuple2(o, t)
                }.fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.a.rounds.size shouldBe 2
            }.unsafeRunSync()
        }

        "Single kollects for different data sources are executed in multiple rounds if they are in a for comprehension" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, List<Int>>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val o = one(CF, 1).bind()
                    val t = many(CF, 3).bind()
                    Tuple2(o, t)
                }.fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.a.rounds.size shouldBe 2
            }.unsafeRunSync()
        }

        "Single kollects combined with cartesian are run in one round" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, List<Int>>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.tupled(one(CF, 1), many(CF, 3)).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.a.rounds.size shouldBe 1
            }.unsafeRunSync()
        }

        "Single kollects combined with traverse are run in one round" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val manies = many(CF, 3).bind()
                    val ones = manies.traverse(monad) { one(CF, it) }.bind()
                    ones
                }.fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.a.rounds.size shouldBe 2
            }.unsafeRunSync()
        }

        "The product of two kollects from the same data source implies batching" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.tupled(one(CF, 1), one(CF, 3)).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.a.rounds.size shouldBe 1
            }.unsafeRunSync()
        }

        "The product of concurrent kollects of the same type implies everything kollected in batches" {
            fun <F> aKollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val a = one(CF, 1).bind()
                    val b = one(CF, 2).bind()
                    val c = one(CF, 3).bind()
                    c
                }.fix()
            }

            fun <F> anotherKollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val a = one(CF, 2).bind()
                    val m = many(CF, 4).bind()
                    val c = one(CF, 3).bind()
                    c
                }.fix()
            }

            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Tuple2<Int, Int>, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.tupled(monad.tupled(aKollect(CF), anotherKollect(CF)).fix(), one(CF, 3)).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.a.rounds.size shouldBe 2
                totalBatches(res.a.rounds) shouldBe 1
                totalKollected(res.a.rounds) shouldBe 3 + 1
            }.unsafeRunSync()
        }
    }
}
