@file:Suppress("FunctionName")

package arrow.effects

import arrow.Kind
import arrow.concurrent.Promise
import arrow.core.Either
import arrow.core.Left
import arrow.core.None
import arrow.core.Option
import arrow.core.Right
import arrow.core.Some
import arrow.core.Success
import arrow.core.Tuple2
import arrow.core.andThen
import arrow.core.right
import arrow.core.some
import arrow.data.EitherT
import arrow.data.EitherTPartialOf
import arrow.data.Kleisli
import arrow.data.KleisliPartialOf
import arrow.data.OptionT
import arrow.data.OptionTPartialOf
import arrow.data.WriterT
import arrow.data.WriterTPartialOf
import arrow.data.fix
import arrow.effects.deferred.Deferred
import arrow.effects.typeclasses.Async
import arrow.instance
import arrow.typeclasses.Monoid
import kollect.arrow.Bracket
import kollect.arrow.ExitCase
import kollect.arrow.TrampolineEC.Companion.immediate
import kollect.arrow.concurrent.FiniteDuration
import kollect.arrow.concurrent.Ref
import kollect.arrow.effects.Timer
import java.util.concurrent.TimeoutException

/**
 * Type class for [[Async]] data types that are cancelable and can be started concurrently.
 */
interface Concurrent<F> : Async<F>, Bracket<F, Throwable> {

    /**
     * Start concurrent execution of the source suspended in
     * the `F` context.
     *
     * Returns a [[Fiber]] that can be used to either join or cancel
     * the running computation, being similar in spirit (but not
     * in implementation) to starting a thread.
     */
    fun <A> start(fa: arrow.Kind<F, A>): arrow.Kind<F, Fiber<F, A>>

    /**
     * Run two tasks concurrently, creating a race between them and returns a
     * pair containing both the winner's successful value and the loser
     * represented as a still-unfinished fiber.
     *
     * If the first task completes in error, then the result will
     * complete in error, the other task being canceled.
     *
     * On usage the user has the option of canceling the losing task.
     *
     * See [[race]] for a simpler version that cancels the loser
     * immediately.
     */
    fun <A, B> racePair(fa: arrow.Kind<F, A>, fb: arrow.Kind<F, B>): arrow.Kind<F, Either<Tuple2<A, Fiber<F, B>>, Tuple2<Fiber<F, A>, B>>>

    /**
     * Run two tasks concurrently and return the first to finish,
     * either in success or error. The loser of the race is canceled.
     *
     * The two tasks are potentially executed in parallel, the winner
     * being the first that signals a result.
     *
     * As an example see [[Concurrent.timeoutTo]]
     *
     * Also see [[racePair]] for a version that does not cancel
     * the loser automatically on successful results.
     */
    fun <A, B> race(fa: arrow.Kind<F, A>, fb: arrow.Kind<F, B>): arrow.Kind<F, Either<A, B>> =
        racePair(fa, fb).flatMap { either ->
            when (either) {
                is Either.Left -> either.a.b.cancel().map { Either.Left(either.a.a) }
                is Either.Right -> either.b.a.cancel().map { Either.Right(either.b.b) }
            }
        }

    /**
     * Creates a cancelable `F[A]` instance that executes an
     * asynchronous process on evaluation.
     *
     * This builder accepts a registration function that is
     * being injected with a side-effectful callback, to be called
     * when the asynchronous process is complete with a final result.
     *
     * The registration function is also supposed to return
     * a [[CancelToken]], which is nothing more than an
     * alias for `F[Unit]`, capturing the logic necessary for
     * canceling the asynchronous process for as long as it
     * is still active.
     */
    fun <A> cancelable(k: ((Either<Throwable, A>) -> Unit) -> CancelToken<F>): arrow.Kind<F, A> =
        Concurrent.defaultCancelable(this, k)

    // TODO uncomment when we can add this data type to Arrow. IO.Pure, IO.RaiseError and the rest of the
    // TODO implementations of the sealed class IO are internal in Arrow.
    /**
     * Inherited from LiftIO, defines a conversion from [[IO]] in terms of the `Concurrent` type class.
     *
     * N.B. expressing this conversion in terms of `Concurrent` and its capabilities means that the resulting `F` is
     * cancelable in case the source `IO` is.
     *
     * To access this implementation as a standalone function, you can use [[Concurrent$.liftIO Concurrent.liftIO]]
     * (on the object companion).
     *//*
    override def liftIO[A](ioa: IO[A]): F[A] = Concurrent.liftIO(ioa)(this)
    */
    companion object {
        // TODO uncomment when we can add this data type to Arrow. IO.Pure, IO.RaiseError and the rest of the
        // TODO implementations of the sealed class IO are internal in Arrow.

        /**
         * Lifts any `IO` value into any data type implementing [[Concurrent]].
         *
         * Compared with [[Async.liftIO]], this version preserves the
         * interruptibility of the given `IO` value.
         *
         * This is the default `Concurrent.liftIO` implementation.
         */
        /*
        def liftIO[F[_], A](ioa: IO[A])(implicit F: Concurrent[F]): F[A] =
        ioa match {
            case Pure(a) => F.pure(a)
            case RaiseError(e) => F.raiseError(e)
            case Delay(thunk) => F.delay(thunk())
            case _ =>
            F.suspend {
                IORunLoop.step(ioa) match {
                    case Pure(a) => F.pure(a)
                    case RaiseError(e) => F.raiseError(e)
                    case async =>
                    F.cancelable(cb => liftIO(async.unsafeRunCancelable(cb))(F))
                }
            }
        }
        */

        /**
         * Returns an effect that either completes with the result of the source within
         * the specified time `duration` or otherwise evaluates the `fallback`.
         *
         * The source is cancelled in the event that it takes longer than
         * the `FiniteDuration` to complete, the evaluation of the fallback
         * happening immediately after that.
         *
         * @param duration is the time span for which we wait for the source to
         *        complete; in the event that the specified time has passed without
         *        the source completing, the `fallback` gets evaluated
         *
         * @param fallback is the task evaluated after the duration has passed and
         *        the source canceled
         */
        fun <F, A> timeoutTo(
            CF: Concurrent<F>,
            timer: Timer<F>,
            fa: arrow.Kind<F, A>,
            duration: FiniteDuration,
            fallback: arrow.Kind<F, A>
        ): arrow.Kind<F, A> = CF.run {
            CF.race(fa, timer.sleep(duration)).flatMap {
                when (it) {
                    is Either.Left -> CF.just(it.a)
                    is Either.Right -> fallback
                }
            }
        }

        /**
         * Lazily memoizes `f`. For every time the returned `F[F[A]]` is
         * bound, the effect `f` will be performed at most once (when the
         * inner `F[A]` is bound the first time).
         *
         * Note: `start` can be used for eager memoization.
         */
        fun <F, A> memoize(CF: Concurrent<F>, f: arrow.Kind<F, A>): arrow.Kind<F, arrow.Kind<F, A>> = CF.run {
            Ref.of<F, Option<Deferred<F, Either<Throwable, A>>>>(CF, None).map { ref ->
                Deferred<F, Either<Throwable, A>>(CF).flatMap { d ->
                    ref.modify { option ->
                        when (option) {
                            is None -> Tuple2(Some(d), f.attempt().flatMap { a -> d.complete(a).map { a } })
                            is Some -> Tuple2(option, option.t.get())
                        }
                    }.flatten().flatMap { either -> either.fold({ CF.raiseError<A>(it) }, { just(it) }) }
                }
            }
        }

        /**
         * Returns an effect that either completes with the result of the source within
         * the specified time `duration` or otherwise raises a `TimeoutException`.
         *
         * The source is cancelled in the event that it takes longer than
         * the specified time duration to complete.
         *
         * @param duration is the time span for which we wait for the source to
         *        complete; in the event that the specified time has passed without
         *        the source completing, a `TimeoutException` is raised
         */
        fun <F, A> timeout(CF: Concurrent<F>, TF: Timer<F>, fa: Kind<F, A>, duration: FiniteDuration): Kind<F, A> =
            timeoutTo(CF, TF, fa, duration, CF.raiseError(TimeoutException(duration.toString())))

        /**
         * Internal API — Cancelable builder derived from [[Async.asyncF]] and [[Bracket.bracketCase]].
         */
        private fun <F, A> defaultCancelable(AF: Concurrent<F>, k: ((Either<Throwable, A>) -> Unit) -> CancelToken<F>): Kind<F, A> =
            AF.async { cb ->
                val latch = Promise<Unit>()
                val latchF = AF.async<Unit> { cb ->
                    latch.future().onComplete(immediate) { cb(Right(Unit)) }
                }
                // Side-effecting call; unfreezes latch in order to allow bracket to finish
                val token = k { result ->
                    latch.complete(Success(Unit))
                    cb(result)
                }
                AF.run {
                    AF.just(token).bracketCase({ a, b ->
                        when (b) {
                            is ExitCase.Cancelled -> a
                            else -> AF.just(Unit)
                        }
                    }, { _ -> latchF })
                }
            }
    }
}

@instance(Concurrent::class)
interface EitherTConcurrent<F, L> : Concurrent<EitherTPartialOf<F, L>> {
    fun CF(): Concurrent<F>

    override fun <A> cancelable(k: ((Either<Throwable, A>) -> Unit) -> CancelToken<EitherTPartialOf<F, L>>): Kind<EitherTPartialOf<F, L>, A> = CF().run {
        EitherT(CF().cancelable(k.andThen { token -> token.fix().value.map { Unit } }).map { it.right() })
    }

    override fun <A> start(fa: Kind<EitherTPartialOf<F, L>, A>): Kind<EitherTPartialOf<F, L>, Fiber<EitherTPartialOf<F, L>, A>> = CF().run {
        EitherT(CF().start(fa.fix().value).map { fiberT(it) }.map { it.right() })
    }

    override fun <A, B> racePair(fa: Kind<EitherTPartialOf<F, L>, A>, fb: Kind<EitherTPartialOf<F, L>, B>): Kind<EitherTPartialOf<F, L>, Either<Tuple2<A, Fiber<EitherTPartialOf<F, L>, B>>, Tuple2<Fiber<EitherTPartialOf<F, L>, A>, B>>> = CF().run {
        EitherT(CF().racePair(fa.fix().value, fb.fix().value).flatMap {
            when (it) {
                is Either.Left -> {
                    val value = it.a.a
                    val fiberB = it.a.b
                    when (value) {
                        is Either.Left -> fiberB.cancel().map { _ -> value as Either.Left<L> }
                        is Either.Right -> CF().just(Right(Left(Tuple2(value.b, fiberT(fiberB)))))
                    }
                }
                is Either.Right -> {
                    val fiberA = it.b.a
                    val value = it.b.b
                    when (value) {
                        is Either.Left -> fiberA.cancel().map { _ -> value as Either.Left<L> }
                        is Either.Right -> CF().just(Right(Right(Tuple2(fiberT(fiberA), value.b))))
                    }
                }
            }
        })
    }

    fun <A> fiberT(fiber: Fiber<F, Either<L, A>>): Fiber<EitherTPartialOf<F, L>, A> = CF().run {
        Fiber(EitherT(fiber.join()), EitherT(fiber.cancel().map { it.right() }))
    }
}

@instance(Concurrent::class)
interface OptionTConcurrent<F> : Concurrent<OptionTPartialOf<F>> {

    fun CF(): Concurrent<F>

    override fun <A> cancelable(k: ((Either<Throwable, A>) -> Unit) -> CancelToken<OptionTPartialOf<F>>): Kind<OptionTPartialOf<F>, A> = CF().run {
        OptionT(CF().cancelable(k.andThen { token -> token.fix().value.map { Unit } }).map { it.some() })
    }

    override fun <A> start(fa: Kind<OptionTPartialOf<F>, A>): Kind<OptionTPartialOf<F>, Fiber<OptionTPartialOf<F>, A>> = CF().run {
        OptionT(CF().start(fa.fix().value).map { fiberT(it) }.map { it.some() })
    }

    override fun <A, B> racePair(fa: Kind<OptionTPartialOf<F>, A>, fb: Kind<OptionTPartialOf<F>, B>): Kind<OptionTPartialOf<F>, Either<Tuple2<A, Fiber<OptionTPartialOf<F>, B>>, Tuple2<Fiber<OptionTPartialOf<F>, A>, B>>> = CF().run {
        OptionT(CF().racePair(fa.fix().value, fb.fix().value).flatMap {
            when (it) {
                is Either.Left -> {
                    val value = it.a.a
                    val fiberB = it.a.b
                    when (value) {
                        is None -> fiberB.cancel().map { _ -> None }
                        is Some -> CF().just(Some(Left(Tuple2(value.t, fiberT(fiberB)))))
                    }
                }
                is Either.Right -> {
                    val fiberA = it.b.a
                    val value = it.b.b
                    when (value) {
                        is None -> fiberA.cancel().map { _ -> None }
                        is Some -> CF().just(Some(Right(Tuple2(fiberT(fiberA), value.t))))
                    }
                }
            }
        })
    }

    fun <A> fiberT(fiber: Fiber<F, Option<A>>): Fiber<OptionTPartialOf<F>, A> = CF().run {
        Fiber(OptionT(fiber.join()), OptionT(fiber.cancel().map { it.some() }))
    }
}

@instance(Concurrent::class)
interface WriterTConcurrent<F, L> : Concurrent<WriterTPartialOf<F, L>> {

    fun CF(): Concurrent<F>

    fun ML(): Monoid<L>

    override fun <A> cancelable(k: ((Either<Throwable, A>) -> Unit) -> CancelToken<WriterTPartialOf<F, L>>): Kind<WriterTPartialOf<F, L>, A> = CF().run {
        WriterT(CF().cancelable(k.andThen { it.fix().value.map { _ -> Unit } }).map { v -> Tuple2(ML().empty(), v) })
    }

    override fun <A> start(fa: Kind<WriterTPartialOf<F, L>, A>): Kind<WriterTPartialOf<F, L>, Fiber<WriterTPartialOf<F, L>, A>> = CF().run {
        WriterT(CF().start(fa.fix().value).map { fiber ->
            Tuple2(ML().empty(), fiberT(fiber))
        })
    }

    override fun <A, B> racePair(fa: Kind<WriterTPartialOf<F, L>, A>, fb: Kind<WriterTPartialOf<F, L>, B>): Kind<WriterTPartialOf<F, L>, Either<Tuple2<A, Fiber<WriterTPartialOf<F, L>, B>>, Tuple2<Fiber<WriterTPartialOf<F, L>, A>, B>>> = CF().run {
        WriterT(CF().racePair(fa.fix().value, fb.fix().value).map {
            when (it) {
                is Either.Left -> {
                    val l = it.a.a.a
                    val value = it.a.a.b
                    val fiber = it.a.b
                    Tuple2(l, Left(Tuple2(value, fiberT(fiber))))
                }
                is Either.Right -> {
                    val fiber = it.b.a
                    val l = it.b.b.a
                    val value = it.b.b.b
                    Tuple2(l, Right(Tuple2(fiberT(fiber), value)))
                }
            }
        })
    }

    fun <A> fiberT(fiber: Fiber<F, Tuple2<L, A>>): Fiber<WriterTPartialOf<F, L>, A> = CF().run {
        Fiber(WriterT(fiber.join()), WriterT(fiber.cancel().map { v -> Tuple2(ML().empty(), v) }))
    }
}

@instance(Concurrent::class)
interface KleisliConcurrent<F, R> : Concurrent<KleisliPartialOf<F, R>> {

    fun CF(): Concurrent<F>

    override fun <A> cancelable(k: ((Either<Throwable, A>) -> Unit) -> CancelToken<KleisliPartialOf<F, R>>): Kind<KleisliPartialOf<F, R>, A> = CF().run {
        Kleisli { r -> CF().cancelable(k.andThen { it.fix().run(r).map { _ -> Unit } }) }
    }

    override fun <A> start(fa: Kind<KleisliPartialOf<F, R>, A>): Kind<KleisliPartialOf<F, R>, Fiber<KleisliPartialOf<F, R>, A>> = CF().run {
        Kleisli { r -> CF().start(fa.fix().run(r)).map { fiberT(it) } }
    }

    override fun <A, B> racePair(fa: Kind<KleisliPartialOf<F, R>, A>, fb: Kind<KleisliPartialOf<F, R>, B>): Kind<KleisliPartialOf<F, R>, Either<Tuple2<A, Fiber<KleisliPartialOf<F, R>, B>>, Tuple2<Fiber<KleisliPartialOf<F, R>, A>, B>>> = CF().run {
        Kleisli { r ->
            CF().racePair(fa.fix().run(r), fb.fix().run(r)).map {
                when (it) {
                    is Either.Left -> {
                        val a = it.a.a
                        val fiber = it.a.b
                        Left(Tuple2(a, fiberT(fiber)))
                    }
                    is Either.Right -> {
                        val fiber = it.b.a
                        val b = it.b.b
                        Right(Tuple2(fiberT(fiber), b))
                    }
                }
            }
        }
    }

    fun <A> fiberT(fiber: Fiber<F, A>): Fiber<KleisliPartialOf<F, R>, A> =
        Fiber(Kleisli { fiber.join() }, Kleisli { fiber.cancel() })
}
