package kollect.extensions

import arrow.core.Right
import arrow.core.Tuple2
import arrow.core.right
import arrow.core.some
import arrow.data.*
import arrow.effects.ForIO
import arrow.effects.IO
import arrow.effects.internal.ForwardCancelable
import arrow.effects.typeclasses.Duration
import arrow.extension
import arrow.typeclasses.Applicative
import arrow.typeclasses.Functor
import arrow.typeclasses.Monoid
import kollect.typeclasses.Clock
import kollect.typeclasses.Timer
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext

@extension
interface EitherTTimer<F, L> : Timer<EitherTPartialOf<F, L>> {
    fun FF(): Functor<F>

    fun TF(): Timer<F>

    fun CF(): Clock<F>

    override fun clock(): Clock<EitherTPartialOf<F, L>> = object : EitherTClock<F, L> {
        override fun clock(): Clock<F> = CF()

        override fun FF(): Functor<F> = FF()
    }

    override fun sleep(duration: Duration): EitherT<F, L, Unit> = FF().run {
        EitherT(TF().sleep(duration).map { it.right() })
    }
}

@extension
interface OptionTTimer<F> : Timer<OptionTPartialOf<F>> {
    fun FF(): Functor<F>

    fun TF(): Timer<F>

    fun CF(): Clock<F>

    override fun clock(): Clock<OptionTPartialOf<F>> = object : OptionTClock<F> {
        override fun FF(): Functor<F> = FF()

        override fun clock(): Clock<F> = CF()
    }

    override fun sleep(duration: Duration): OptionT<F, Unit> = FF().run {
        OptionT(TF().sleep(duration).map { it.some() })
    }
}

@extension
interface WriterTTimer<F, L> : Timer<WriterTPartialOf<F, L>> {

    fun AF(): Applicative<F>

    fun ML(): Monoid<L>

    fun TF(): Timer<F>

    fun CF(): Clock<F>

    override fun clock(): Clock<WriterTPartialOf<F, L>> = object : WriterTClock<F, L> {
        override fun AF(): Applicative<F> = AF()

        override fun ML(): Monoid<L> = ML()

        override fun clock(): Clock<F> = CF()
    }

    override fun sleep(duration: Duration): WriterT<F, L, Unit> = AF().run {
        WriterT(TF().sleep(duration).map { v -> Tuple2(ML().empty(), v) })
    }
}

@extension
interface KleisliTimer<F, R> : Timer<KleisliPartialOf<F, R>> {

    fun TF(): Timer<F>

    fun CF(): Clock<F>

    override fun clock(): Clock<KleisliPartialOf<F, R>> = object : KleisliClock<F, R> {
        override fun clock(): Clock<F> = CF()
    }

    override fun sleep(duration: Duration): Kleisli<F, R, Unit> =
            Kleisli { TF().sleep(duration) }
}

@extension
interface IOTimer : Timer<ForIO> {

    fun ec(): CoroutineContext

    override fun clock(): Clock<ForIO> = object : IOClock {}

    override fun sleep(duration: Duration): IO<Unit> =
            IO.async { conn, cb ->
                // Doing what IO.cancelable does
                val ref = ForwardCancelable()
                conn.push(ref.cancel())

                // If not cancelled yet, schedule the task and wire the cancellation to scheduler
                if (!conn.isCanceled()) {
                    val scheduledFuture = scheduler.schedule({
                        conn.pop()
                        cb(Right(Unit))
                    }, duration.nanoseconds, TimeUnit.NANOSECONDS)

                    ref.complete(IO {
                        scheduledFuture.cancel(false)
                        Unit
                    })
                } else ref.complete(IO.unit)
            }
}

private val scheduler: ScheduledExecutorService by lazy {
    Executors.newScheduledThreadPool(2) { r ->
        Thread(r).apply {
            name = "arrow-effects-scheduler-$id"
            isDaemon = true
        }
    }
}
