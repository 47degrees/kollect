package kollect.test

import arrow.Kind
import arrow.core.Option
import arrow.core.Some
import arrow.data.ForListK
import arrow.data.ListK
import arrow.data.extensions.list.foldable.foldLeft
import arrow.data.extensions.list.traverse.sequence
import arrow.data.extensions.list.traverse.traverse
import arrow.data.k
import arrow.effects.IO
import arrow.effects.extensions.io.concurrent.concurrent
import arrow.effects.extensions.io.functor.map
import arrow.effects.typeclasses.Concurrent
import io.kotlintest.runner.junit4.KotlinTestRunner
import io.kotlintest.shouldBe
import kollect.*
import kollect.extensions.io.timer.timer
import kollect.extensions.kollect.monad.monad
import org.junit.runner.RunWith
import kotlin.coroutines.EmptyCoroutineContext

@RunWith(KotlinTestRunner::class)
class KollectBatchingTests : KollectSpec() {

    data class BatchedDataSeq(val id: Int)

    object MaxBatchSourceSeq : DataSource<BatchedDataSeq, Int> {

        override fun name(): String = "BatchSourceSeq"

        override fun <F> fetch(CF: Concurrent<F>, id: BatchedDataSeq): Kind<F, Option<Int>> =
                CF.just(Some(id.id))

        override fun maxBatchSize(): Option<Int> = Some(2)

        override fun batchExecution(): BatchExecution = Sequentially
    }

    data class BatchedDataPar(val id: Int)

    object MaxBatchSourcePar : DataSource<BatchedDataPar, Int> {
        override fun name(): String = "BatchSourcePar"

        override fun <F> fetch(CF: Concurrent<F>, id: BatchedDataPar): Kind<F, Option<Int>> =
                CF.just(Some(id.id))

        override fun maxBatchSize(): Option<Int> = Some(2)

        override fun batchExecution(): BatchExecution = InParallel
    }

    fun <F> fetchBatchedDataSeq(CF: Concurrent<F>, id: Int): Kollect<F, Int> =
            Kollect(CF, BatchedDataSeq(id), MaxBatchSourceSeq)

    fun <F> fetchBatchedDataPar(CF: Concurrent<F>, id: Int): Kollect<F, Int> =
            Kollect(CF, BatchedDataPar(id), MaxBatchSourcePar)

    init {

        "A large kollect to a datasource with a maximum batch size is split and executed in sequence" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, List<Int>> =
                    (1 until 6).toList().k().traverse(Kollect.monad<F, Int>(CF)) {
                        fetchBatchedDataSeq(CF, it)
                    }.fix()


            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.b shouldBe listOf(1, 2, 3, 4, 5)
                res.a.rounds.size shouldBe 1
                totalKollected(res.a.rounds) shouldBe 5
                totalBatches(res.a.rounds) shouldBe 3
            }.unsafeRunSync()
        }

        "A large kollect to a datasource with a maximum batch size is split and executed in parallel" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, List<Int>> =
                    (1 until 6).toList().k().traverse(Kollect.monad<F, Int>(CF)) {
                        fetchBatchedDataPar(CF, it)
                    }.fix()


            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.b shouldBe listOf(1, 2, 3, 4, 5)
                res.a.rounds.size shouldBe 1
                totalKollected(res.a.rounds) shouldBe 5
                totalBatches(res.a.rounds) shouldBe 3
            }.unsafeRunSync()
        }

        "Kollects to data sources with a maximum batch size should be split and executed in parallel and sequentially" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, ListK<Int>> {
                val monad = Kollect.monad<F, ListK<Int>>(CF)

                val a = (1 until 6).toList().k().traverse(monad) {
                    fetchBatchedDataPar(CF, it)
                }.fix()

                val b = (1 until 6).toList().k().traverse(monad) {
                    fetchBatchedDataSeq(CF, it)
                }.fix()

                return monad.run { a.followedBy(b).fix() }
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.b shouldBe listOf(1, 2, 3, 4, 5)
                res.a.rounds.size shouldBe 2
                totalKollected(res.a.rounds) shouldBe 5 + 5
                totalBatches(res.a.rounds) shouldBe 3 + 3
            }.unsafeRunSync()
        }

        "A large (many) kollect to a datasource with a maximum batch size is split and executed in sequence" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, ListK<Int>>(CF)
                return listOf(fetchBatchedDataSeq(CF, 1), fetchBatchedDataSeq(CF, 2), fetchBatchedDataSeq(CF, 3))
                        .sequence(monad).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.b shouldBe listOf(1, 2, 3)
                res.a.rounds.size shouldBe 1
                totalKollected(res.a.rounds) shouldBe 3
                totalBatches(res.a.rounds) shouldBe 2
            }.unsafeRunSync()
        }

        "A large (many) kollect to a datasource with a maximum batch size is split and executed in parallel" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, ListK<Int>>(CF)
                return listOf(fetchBatchedDataPar(CF, 1), fetchBatchedDataPar(CF, 2), fetchBatchedDataPar(CF, 3))
                        .sequence(monad).fix()
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.b shouldBe listOf(1, 2, 3)
                res.a.rounds.size shouldBe 1
                totalKollected(res.a.rounds) shouldBe 3
                totalBatches(res.a.rounds) shouldBe 2
            }.unsafeRunSync()
        }

        "Very deep kollects don't overflow stack or heap" {
            val depth = 200
            val list = (1..depth).toList()

            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Kind<ForListK, Int>> {
                val monad = Kollect.monad<F, ListK<Int>>(CF)
                val a = list.map { x -> (0 until x).toList().traverse(monad) { fetchBatchedDataSeq(CF, it) } }
                return a.foldLeft(monad.just(ListK.empty<Int>() as Kind<ForListK, Int>)) { acc, i ->
                    val current = i.fix()
                    monad.run { acc.flatMap { current } }
                }
            }

            val io = Kollect.runEnv(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { res ->
                res.b shouldBe (0 until depth).toList()
                res.a.rounds.size shouldBe depth
            }.unsafeRunSync()
        }
    }
}
