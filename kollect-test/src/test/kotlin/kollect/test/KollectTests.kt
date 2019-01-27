package kollect.test

import arrow.core.Tuple2
import arrow.effects.ForIO
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
import kollect.test.TestHelper.one
import org.junit.runner.RunWith
import kotlin.coroutines.EmptyCoroutineContext

@RunWith(KotlinTestRunner::class)
class KollectTests : AbstractStringSpec() {
    init {
        "We can use Kollect inside a for comprehension" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Int, Int>> =
                    Kollect.monad<F, Int>(CF).binding {
                        val o = one(CF, 1).bind()
                        val t = one(CF, 2).bind()
                        Tuple2(o, t)
                    }.fix()

            val io = Kollect.run<ForIO>()(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe Tuple2(1, 2)
        }
    }
}
