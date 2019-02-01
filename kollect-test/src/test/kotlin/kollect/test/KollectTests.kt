package kollect.test

import arrow.Kind
import arrow.core.Either
import arrow.core.None
import arrow.core.Option
import arrow.core.Tuple2
import arrow.data.*
import arrow.data.extensions.list.traverse.sequence
import arrow.data.extensions.list.traverse.traverse
import arrow.effects.IO
import arrow.effects.extensions.io.applicativeError.attempt
import arrow.effects.extensions.io.applicativeError.handleErrorWith
import arrow.effects.extensions.io.concurrent.concurrent
import arrow.effects.extensions.io.functor.map
import arrow.effects.fix
import arrow.effects.typeclasses.Concurrent
import arrow.typeclasses.Monad
import io.kotlintest.runner.junit4.KotlinTestRunner
import io.kotlintest.shouldBe
import kollect.*
import kollect.KollectException.MissingIdentity
import kollect.extensions.io.timer.timer
import kollect.extensions.kollect.monad.monad
import kollect.test.TestHelper.Never
import kollect.test.TestHelper.NeverSource
import kollect.test.TestHelper.One
import kollect.test.TestHelper.OneSource
import kollect.test.TestHelper.anotherOne
import kollect.test.TestHelper.many
import kollect.test.TestHelper.never
import kollect.test.TestHelper.one
import org.junit.Assert.assertTrue
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
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Tuple2<Int, Tuple2<Int, Int>>, Int>> {
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
            envRounds.size shouldBe 2
            totalBatches(envRounds) shouldBe 2
            totalFetched(envRounds) shouldBe 5
        }

        "Every level of joined concurrent kollects is combined and batched" {
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

            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.tupled(aKollect(CF), anotherKollect(CF)).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe Tuple2(1, 2)
            envRounds.size shouldBe 2
            totalBatches(envRounds) shouldBe 2
            totalFetched(envRounds) shouldBe 4
        }

        "Every level of sequenced concurrent kollects is batched" {
            fun <F> aKollect(CF: Concurrent<F>): Kollect<F, ListK<Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val a = listOf(2, 3, 4).k().traverse(monad) { one(CF, it) }.bind() // round 1
                    val b = listOf(0, 1).k().traverse(monad) { many(CF, it) }.bind() // round 2
                    val c = listOf(9, 10, 11).k().traverse(monad) { one(CF, it) }.bind() // round 3
                    c
                }.fix()
            }

            fun <F> anotherKollect(CF: Concurrent<F>): Kollect<F, ListK<Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val a = listOf(5, 6, 7).k().traverse(monad) { one(CF, it) }.bind() // round 1
                    val b = listOf(2, 3).k().traverse(monad) { many(CF, it) }.bind() // round 2
                    val c = listOf(12, 13, 14).k().traverse(monad) { one(CF, it) }.bind() // round 3
                    c
                }.fix()
            }

            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Tuple2<ListK<Int>, ListK<Int>>, ListK<Int>>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.tupled(monad.tupled(aKollect(CF), anotherKollect(CF)), listOf(15, 16, 17).k().traverse(monad) { one(CF, it) }).fix() // round 1
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe Tuple2(Tuple2(listOf(9, 10, 11), listOf(12, 13, 14)), listOf(15, 16, 17))
            envRounds.size shouldBe 3
            totalBatches(envRounds) shouldBe 3
            totalFetched(envRounds) shouldBe 9 + 4 + 6
        }

        "The product of two kollects from the same data source implies batching" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.tupled(one(CF, 1), one(CF, 3)).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe Tuple2(1, 3)
            envRounds.size shouldBe 1
            totalBatches(envRounds) shouldBe 1
            totalFetched(envRounds) shouldBe 2
        }

        "Sequenced kollects are run concurrently" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return listOf(one(CF, 1), one(CF, 2), one(CF, 3), anotherOne(CF, 4), anotherOne(CF, 5)).k().sequence(monad).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe listOf(1, 2, 3, 4, 5)
            envRounds.size shouldBe 1
            totalBatches(envRounds) shouldBe 2
        }

        "Sequenced kollects are deduped" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return listOf(one(CF, 1), one(CF, 2), one(CF, 1)).k().sequence(monad).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe listOf(1, 2, 1)
            envRounds.size shouldBe 1
            totalBatches(envRounds) shouldBe 1
        }

        "Traversals are batched" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return listOf(1, 2, 3).k().traverse(monad) { one(CF, it) }.fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe listOf(1, 2, 3)
            envRounds.size shouldBe 1
            totalBatches(envRounds) shouldBe 1
        }

        "Duplicated sources are only kollected once" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return listOf(1, 2, 1).k().traverse(monad) { one(CF, it) }.fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe listOf(1, 2, 1)
            envRounds.size shouldBe 1
            totalFetched(envRounds) shouldBe 2
        }

        "Sources that can be kollected concurrently inside a for comprehension will be" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val v = Kollect.just(CF, listOf(1, 2, 1)).bind()
                    val result = v.traverse(monad) { one(CF, it) }.bind()
                    result
                }.fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe listOf(1, 2, 1)
            envRounds.size shouldBe 1
            totalFetched(envRounds) shouldBe 2
        }

        "Pure Kollects allow to explore further in the Kollect"  {
            fun <F> aKollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val a = Kollect.just(CF, 2).bind()
                    val b = one(CF, 3).bind()
                    a + b
                }.fix()
            }

            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, Int>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.tupled(one(CF, 1), aKollect(CF)).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe Tuple2(1, 5)
            envRounds.size shouldBe 1
            totalFetched(envRounds) shouldBe 2
        }

        // Caching

        "Elements are cached and thus not kollected more than once" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val aOne = one(CF, 1).bind()
                    val anotherOne = one(CF, 1).bind()
                    one(CF, 1).bind()
                    one(CF, 1).bind()
                    one(CF, 1).bind()
                    one(CF, 1).bind()
                    listOf(1, 2, 3).k().traverse(monad) { one(CF, it) }.bind()
                    one(CF, 1).bind()
                    aOne + anotherOne
                }.fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe 2
            totalFetched(envRounds) shouldBe 3
        }

        "Batched elements are cached and thus not kollected more than once" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    listOf(1, 2, 3).k().traverse(monad) { one(CF, it) }.bind()
                    val aOne = one(CF, 1).bind()
                    val anotherOne = one(CF, 1).bind()
                    one(CF, 1).bind()
                    one(CF, 2).bind()
                    one(CF, 3).bind()
                    one(CF, 1).bind()
                    one(CF, 1).bind()

                    aOne + anotherOne
                }.fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe 2
            envRounds.size shouldBe 1
            totalFetched(envRounds) shouldBe 3
        }

        "Elements that are cached won't be kollected" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val aOne = one(CF, 1).bind()
                    val anotherOne = one(CF, 1).bind()
                    one(CF, 1).bind()
                    one(CF, 2).bind()
                    one(CF, 3).bind()
                    one(CF, 1).bind()
                    listOf(1, 2, 3).k().traverse(monad) { one(CF, it) }.bind()
                    one(CF, 1).bind()

                    aOne + anotherOne
                }.fix()
            }

            fun <F> cache(CF: Concurrent<F>) = InMemoryCache(CF, mapOf(
                    Tuple2(OneSource.name(), DataSourceId(One(1))) to 1.dsResult(),
                    Tuple2(OneSource.name(), DataSourceId(One(2))) to 2.dsResult(),
                    Tuple2(OneSource.name(), DataSourceId(One(3))) to 3.dsResult()
            ))

            val cf = IO.concurrent()
            val io = Kollect.runEnv(cf, IO.timer(EmptyCoroutineContext), kollect(cf), cache(cf))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe 2
            envRounds.size shouldBe 0
            totalFetched(envRounds) shouldBe 0
        }

        "Kollect#run accepts a cache as the second (optional) parameter" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val aOne = one(CF, 1).bind()
                    val anotherOne = one(CF, 1).bind()
                    one(CF, 1).bind()
                    one(CF, 2).bind()
                    one(CF, 3).bind()
                    one(CF, 1).bind()
                    listOf(1, 2, 3).k().traverse(monad) { one(CF, it) }.bind()
                    one(CF, 1).bind()

                    aOne + anotherOne
                }.fix()
            }

            fun <F> cache(CF: Concurrent<F>) = InMemoryCache(CF, mapOf(
                    Tuple2(OneSource.name(), DataSourceId(One(1))) to 1.dsResult(),
                    Tuple2(OneSource.name(), DataSourceId(One(2))) to 2.dsResult(),
                    Tuple2(OneSource.name(), DataSourceId(One(3))) to 3.dsResult()
            ))

            val cf = IO.concurrent()
            val io = Kollect.run(cf, IO.timer(EmptyCoroutineContext), kollect(cf), cache(cf))
            val res = io.fix().unsafeRunSync()

            res shouldBe 2
        }

        "Kollect#runCache accepts a cache as the second (optional) parameter" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val aOne = one(CF, 1).bind()
                    val anotherOne = one(CF, 1).bind()
                    one(CF, 1).bind()
                    one(CF, 2).bind()
                    one(CF, 3).bind()
                    one(CF, 1).bind()
                    listOf(1, 2, 3).k().traverse(monad) { one(CF, it) }.bind()
                    one(CF, 1).bind()

                    aOne + anotherOne
                }.fix()
            }

            fun <F> cache(CF: Concurrent<F>) = InMemoryCache(CF, mapOf(
                    Tuple2(OneSource.name(), DataSourceId(One(1))) to 1.dsResult(),
                    Tuple2(OneSource.name(), DataSourceId(One(2))) to 2.dsResult(),
                    Tuple2(OneSource.name(), DataSourceId(One(3))) to 3.dsResult()
            ))

            val cf = IO.concurrent()
            val io = Kollect.runCache(cf, IO.timer(EmptyCoroutineContext), kollect(cf), cache(cf))
            val res = io.fix().unsafeRunSync()

            res.b shouldBe 2
        }

        "Kollect#runCache works without the optional cache parameter" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val aOne = one(CF, 1).bind()
                    val anotherOne = one(CF, 1).bind()
                    one(CF, 1).bind()
                    one(CF, 2).bind()
                    one(CF, 3).bind()
                    one(CF, 1).bind()
                    listOf(1, 2, 3).k().traverse(monad) { one(CF, it) }.bind()
                    one(CF, 1).bind()

                    aOne + anotherOne
                }.fix()
            }

            val cf = IO.concurrent()
            val io = Kollect.runCache(cf, IO.timer(EmptyCoroutineContext), kollect(cf))
            val res = io.fix().unsafeRunSync()

            res.b shouldBe 2
        }

        data class ForgetfulCache<F>(val MF: Monad<F>) : DataSourceCache<F> {
            override fun <I : Any, A : Any> insert(i: I, v: A, ds: DataSource<I, A>): Kind<F, DataSourceCache<F>> {
                return MF.just(this)
            }

            override fun <I : Any, A : Any> lookup(i: I, ds: DataSource<I, A>): Kind<F, Option<A>> {
                return MF.just(None)
            }
        }

        fun <F> forgetfulCache(CF: Concurrent<F>) = ForgetfulCache(CF)

        "We can use a custom cache that discards elements" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val aOne = one(CF, 1).bind()
                    val anotherOne = one(CF, 1).bind()
                    one(CF, 1).bind()
                    one(CF, 2).bind()
                    one(CF, 3).bind()
                    one(CF, 1).bind()
                    one(CF, 1).bind()

                    aOne + anotherOne
                }.fix()
            }

            val cf = IO.concurrent()
            val io = Kollect.runEnv(cf, IO.timer(EmptyCoroutineContext), kollect(cf), forgetfulCache(cf))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe 2
            envRounds.size shouldBe 7
            totalFetched(envRounds) shouldBe 7
        }

        "We can use a custom cache that discards elements together with concurrent kollects" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Int> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val aOne = one(CF, 1).bind()
                    val anotherOne = one(CF, 1).bind()
                    one(CF, 1).bind()
                    one(CF, 2).bind()
                    listOf(1, 2, 3).k().traverse(monad) { one(CF, it) }.bind()
                    one(CF, 3).bind()
                    one(CF, 1).bind()
                    one(CF, 1).bind()

                    aOne + anotherOne
                }.fix()
            }

            val cf = IO.concurrent()
            val io = Kollect.runEnv(cf, IO.timer(EmptyCoroutineContext), kollect(cf), forgetfulCache(cf))
            val res = io.fix().unsafeRunSync()

            val result = res.b
            val envRounds = res.a.rounds

            result shouldBe 2
            envRounds.size shouldBe 8
            totalFetched(envRounds) shouldBe 10
        }

        // Errors

        "Data sources with errors throw kollect failures" {
            val cf = IO.concurrent()
            val io = Kollect.run(cf, IO.timer(EmptyCoroutineContext), never(cf))
            io.attempt().map { attempt ->
                assertTrue(attempt is Either.Left)
                when (attempt) {
                    is Either.Left -> {
                        assertTrue(attempt.a is NoStackTrace)
                        assertTrue(attempt.a.message == MissingIdentity::class.java.simpleName)
                    }
                }
            }.unsafeRunSync()
        }

        "Data sources with errors throw kollect failures that can be handled" {
            val cf = IO.concurrent()
            val io = Kollect.run(cf, IO.timer(EmptyCoroutineContext), never(cf))

            io.handleErrorWith { IO.just(42) }
                    .map { it shouldBe 42 }
                    .unsafeRunSync()
        }

        "Data sources with errors won't fail if they're cached" {
            fun <F> cache(CF: Concurrent<F>) = InMemoryCache(CF, mapOf(
                    Tuple2(NeverSource.name(), DataSourceId(Never)) to 1.dsResult()
            ))

            val cf = IO.concurrent()
            val io = Kollect.run(cf, IO.timer(EmptyCoroutineContext), never(cf), cache(cf))

            io.map { it shouldBe 1 }.unsafeRunSync()
        }

        fun <F> fetchError(CF: Concurrent<F>): Kollect<F, Int> =
                Kollect.error(CF, TestHelper.AnException)

        
    }
}
