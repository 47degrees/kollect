package kollect.test

import arrow.Kind
import arrow.core.Tuple2
import arrow.data.ForListK
import arrow.data.NonEmptyList
import arrow.data.extensions.list.traverse.sequence
import arrow.data.extensions.list.traverse.traverse
import arrow.data.fix
import arrow.effects.IO
import arrow.effects.extensions.io.concurrent.concurrent
import arrow.effects.fix
import arrow.effects.typeclasses.Concurrent
import io.kotlintest.runner.junit4.KotlinTestRunner
import io.kotlintest.shouldBe
import kollect.Kollect
import kollect.KollectQuery
import kollect.extensions.io.timer.timer
import kollect.extensions.kollect.monad.monad
import kollect.fix
import kollect.test.TestHelper.anotherOne
import kollect.test.TestHelper.many
import kollect.test.TestHelper.one
import org.junit.runner.RunWith
import kotlin.coroutines.EmptyCoroutineContext

@RunWith(KotlinTestRunner::class)
class KollectTests : KollectSpec() {
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

        "We can use Kollect as an applicative" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> =
                    Kollect.monad<F, Int>(CF).map(one(CF, 1), one(CF, 2), one(CF, 3)) { t -> t.a + t.b + t.c }.fix()

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe 6
        }

        "We can traverse over a list with a Kollect for each element" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, List<Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val manies = many(CF, 3).bind()
                    val ones = manies.traverse(monad) { a -> one(CF, a) }.bind()
                    ones.fix()
                }.fix()
            }

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe listOf(0, 1, 2)
        }

        "We can depend on previous computations of Kollect values" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> =
                    Kollect.monad<F, Int>(CF).binding {
                        val o = one(CF, 1).bind()
                        val t = one(CF, o + 1).bind()
                        o + t
                    }.fix()

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe 3
        }

        "We can collect a list of Kollect into one" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> =
                    listOf(one(CF, 1), one(CF, 2), one(CF, 3)).sequence(Kollect.monad<F, Int>(CF)).fix()

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe listOf(1, 2, 3)
        }

        "We can collect a list of Kollects with heterogeneous sources" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> =
                    listOf(one(CF, 1), one(CF, 2), one(CF, 3), anotherOne(CF, 4), anotherOne(CF, 5))
                            .sequence(Kollect.monad<F, Int>(CF)).fix()

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe listOf(1, 2, 3, 4, 5)
        }

        "We can collect the results of a traversal" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return listOf(1, 2, 3).traverse(monad) { one(CF, it) }.fix()
            }

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe listOf(1, 2, 3)
        }

        // Execution model

        "Monadic bind implies sequential execution" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, Int>> =
                    Kollect.monad<F, Int>(CF).binding {
                        val o = one(CF, 1).bind()
                        val t = one(CF, 2).bind()
                        Tuple2(o, t)
                    }.fix()

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res.a.rounds.size shouldBe 2
            res.b shouldBe Tuple2(1, 2)
        }

        "Traversals are implicitly batched" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val manies = many(CF, 3).bind()
                    val ones = manies.traverse(monad) { one(CF, it) }.bind()
                    ones
                }.fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res.a.rounds.size shouldBe 2
            res.b shouldBe listOf(0, 1, 2)
        }

        "Sequencing is implicitly batched" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return listOf(one(CF, 1), one(CF, 2), one(CF, 3)).sequence(monad).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val envRounds = res.a.rounds
            res.b shouldBe listOf(1, 2, 3)
            envRounds.size shouldBe 1
            totalFetched(envRounds) shouldBe 3
            totalBatches(envRounds) shouldBe 1
        }

        "Identities are deduped when batched" {
            val sources = listOf(1, 1, 2)
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> =
                    sources.traverse(Kollect.monad<F, Int>(CF)) { one(CF, it) }.fix()

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds
            val firstRoundQueries = envRounds[0].queries
            val firstRoundFirstQuery = firstRoundQueries[0]

            result shouldBe sources
            envRounds.size shouldBe 1
            firstRoundQueries.size shouldBe 1
            firstRoundFirstQuery.request shouldBe KollectQuery.Batch(
                    NonEmptyList(TestHelper.One(1), listOf(TestHelper.One(2))),
                    (firstRoundFirstQuery.request as KollectQuery.Batch<TestHelper.One, Int>).ds)
        }

        "The product of two fetches implies parallel fetching" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, List<Int>>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.tupled(one(CF, 1), many(CF, 3)).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe Tuple2(1, listOf(0, 1, 2))
            envRounds.size shouldBe 1
            envRounds[0].queries.size shouldBe 2
        }

        "Concurrent fetching calls batches only when it can" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, List<Int>>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.tupled(one(CF, 1), many(CF, 3)).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe Tuple2(1, listOf(0, 1, 2))
            envRounds.size shouldBe 1
            totalBatches(envRounds) shouldBe 0
        }

        "Concurrent fetching performs requests to multiple data sources in parallel" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Tuple2<Int, List<Int>>, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.tupled(monad.tupled(one(CF, 1), many(CF, 2)).fix(), anotherOne(CF, 3)).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe Tuple2(Tuple2(1, listOf(0, 1)), 3)
            envRounds.size shouldBe 1
            totalBatches(envRounds) shouldBe 0
        }

        "The product of concurrent kollects implies everything kollected concurrently" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Tuple2<Int, Tuple2<Int,Int>>, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                val tupled2and3 = monad.tupled(one(CF, 2), one(CF, 3)).fix()
                val tupled1and2and3 = monad.tupled(one(CF, 1), tupled2and3).fix()

                return monad.tupled(tupled1and2and3, one(CF, 4)).fix()
            }


            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe Tuple2(Tuple2(1, Tuple2(2, 3)), 4)
            envRounds.size shouldBe 1
            totalBatches(envRounds) shouldBe 1
            totalFetched(envRounds) shouldBe 4
        }

        "The product of concurrent kollects of the same type implies everything kollected in a single batch" {
            fun <F> aKollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val a = one(CF, 1).bind() // round 1
                    val b = many(CF, 1).bind() // round 2
                    val c = one(CF, 1).bind()
                    c
                }.fix()
            }

            fun <F> anotherKollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val a = one(CF, 2).bind() // round 1
                    val b = many(CF, 2).bind() // round 2
                    val c = one(CF, 2).bind()
                    c
                }.fix()
            }

            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Tuple2<Int, Int>, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.tupled(monad.tupled(aKollect(CF), anotherKollect(CF)), one(CF, 3)).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe Tuple2(Tuple2(1, 2), 3)
            envRounds.size shouldBe 2 // 3
            totalBatches(envRounds) shouldBe 2 // 3
            totalFetched(envRounds) shouldBe 5 // 7
        }
    }
}
