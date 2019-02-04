package kollect.test

import arrow.effects.ForIO
import arrow.effects.IO
import arrow.effects.extensions.io.applicativeError.handleError
import arrow.effects.extensions.io.concurrent.concurrent
import arrow.effects.fix
import arrow.effects.typeclasses.Concurrent
import io.kotlintest.runner.junit4.KotlinTestRunner
import io.kotlintest.shouldBe
import kollect.Kollect
import kollect.extensions.io.timer.timer
import kollect.fetch
import org.junit.runner.RunWith
import kotlin.coroutines.EmptyCoroutineContext

@RunWith(KotlinTestRunner::class)
class KollectSyntaxTests : KollectSpec() {

    init {

        "fetch syntax allows lifting of any value to the context of a fetch" {
            val concurrent = IO.concurrent()
            Kollect.just(concurrent, 42) shouldBe concurrent.fetch(42)
        }

        "fetch syntax can be used on infix position" {
            val concurrent = IO.concurrent()
            Kollect.just(concurrent, 42) shouldBe (concurrent fetch 42)
        }

        "fetch syntax allows lifting of any `Throwable` as a failure on a kollect" {
            class Ex : RuntimeException()

            fun <F> f1(CF: Concurrent<F>) = Kollect.error<F, Int>(CF, Ex())
            fun <F> f2(CF: Concurrent<F>): Kollect<F, Int> = CF.fetch(Ex())

            val cf = IO.concurrent()
            val io1 = Kollect.runEnv(cf, IO.timer(EmptyCoroutineContext), f1(cf))
            val io2 = Kollect.runEnv(cf, IO.timer(EmptyCoroutineContext), f2(cf))

            val e1 = io1.handleError { err -> 42 }
            val e2 = io2.handleError { err -> 42 }

            val io = cf.map(e1, e2) { tuple -> tuple.a shouldBe tuple.b }.fix()
            io.unsafeRunSync()
        }

        "fetch throwable syntax can be used in infix position" {
            class Ex : RuntimeException()

            fun <F> f1(CF: Concurrent<F>) = Kollect.error<F, Int>(CF, Ex())
            fun <F> f2(CF: Concurrent<F>): Kollect<F, Int> = CF fetch Ex()

            val cf = IO.concurrent()
            val io1 = Kollect.runEnv(cf, IO.timer(EmptyCoroutineContext), f1(cf))
            val io2 = Kollect.runEnv(cf, IO.timer(EmptyCoroutineContext), f2(cf))

            val e1 = io1.handleError { err -> 42 }
            val e2 = io2.handleError { err -> 42 }

            val io = cf.map(e1, e2) { tuple -> tuple.a shouldBe tuple.b }.fix()
            io.unsafeRunSync()
        }
    }
}
