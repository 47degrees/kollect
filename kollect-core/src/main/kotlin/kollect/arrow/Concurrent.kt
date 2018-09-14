package arrow.effects

import arrow.Kind
import arrow.concurrent.Promise
import arrow.core.Either
import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.core.Tuple2
import arrow.data.EitherTPartialOf
import arrow.effects.typeclasses.Async
import arrow.instance
import kollect.arrow.ExecutionContext
import kollect.arrow.concurrent.FiniteDuration
import kollect.arrow.concurrent.Ref
import kollect.arrow.effects.Timer
import java.util.concurrent.TimeoutException

/**
 * Type class for [[Async]] data types that are cancelable and can be started concurrently.
 */
interface Concurrent<F> : Async<F> {

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
     * On usage the user has the option of canceling the losing task,
     * this being equivalent with plain [[race]]:
     *
     * {{{
     *   val ioA: IO[A] = ???
     *   val ioB: IO[B] = ???
     *
     *   Concurrent[IO].racePair(ioA, ioB).flatMap {
     *     case Left((a, fiberB)) =>
     *       fiberB.cancel.map(_ => a)
     *     case Right((fiberA, b)) =>
     *       fiberA.cancel.map(_ => b)
     *   }
     * }}}
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
     *
     * Example:
     *
     * {{{
     *   import java.util.concurrent.ScheduledExecutorService
     *   import scala.concurrent.duration._
     *
     *   def sleep[F[_]](d: FiniteDuration)
     *     (implicit F: Concurrent[F], ec: ScheduledExecutorService): F[Unit] = {
     *
     *     F.cancelable { cb =>
     *       // Schedules task to run after delay
     *       val run = new Runnable { def run() = cb(Right(())) }
     *       val future = ec.schedule(run, d.length, d.unit)
     *
     *       // Cancellation logic, suspended in F
     *       F.delay(future.cancel(true))
     *     }
     *   }
     * }}}
     */

    fun <A> cancelable(k: ((Either<Throwable, A>) -> Unit) -> CancelToken<F>): arrow.Kind<F, A> =
        Concurrent.defaultCancelable(this, k)

    // TODO uncomment when we can add this data type to Arrow. IO.Pure, IO.RaiseError and the rest of the
    // TODO implementations of the sealed class IO are internal in Arrow.
    /**
     * Inherited from [[LiftIO]], defines a conversion from [[IO]]
     * in terms of the `Concurrent` type class.
     *
     * N.B. expressing this conversion in terms of `Concurrent` and
     * its capabilities means that the resulting `F` is cancelable in
     * case the source `IO` is.
     *
     * To access this implementation as a standalone function, you can
     * use [[Concurrent$.liftIO Concurrent.liftIO]]
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
         * Internal API — Cancelable builder derived from
         * [[Async.asyncF]] and [[Bracket.bracketCase]].
         */
        private fun <F, A> defaultCancelable(executor: ExecutionContext, AF: Async<F>, k: ((Either<Throwable, A>) -> Unit) -> CancelToken<F>): Kind<F, A> =
            AF.async { cb ->
                val latch = Promise<Unit>()
                val latchF = AF.async<Unit> { cb -> latch.future().onComplete(executor, { cb(rightUnit))(immediate) } }
                // Side-effecting call; unfreezes latch in order to allow bracket to finish
                val token = k { result =>
                    latch.complete(successUnit)
                    cb(result)
                }
                F.bracketCase(F.pure(token))(_ => latchF) {
                case (cancel, Canceled) => cancel
                case _ => F.unit
            }
        }
        }
    }
        /**
         * [[Concurrent]] instance built for `cats.data.OptionT` values initialized
         * with any `F` data type that also implements `Concurrent`.
         *//*

        implicit def catsOptionTConcurrent[F[_]: Concurrent]: Concurrent[OptionT[F, ?]] =
        new OptionTConcurrent[F]
        { def F = Concurrent [F] }

        */
        /**
         * [[Concurrent]] instance built for `cats.data.Kleisli` values initialized
         * with any `F` data type that also implements `Concurrent`.
         *//*

        implicit def catsKleisliConcurrent[F[_]: Concurrent, R]: Concurrent[Kleisli[F, R, ?]] =
        new KleisliConcurrent[F, R]
        { def F = Concurrent [F] }

        */
        /**
         * [[Concurrent]] instance built for `cats.data.WriterT` values initialized
         * with any `F` data type that also implements `Concurrent`.
         *//*

        implicit def catsWriterTConcurrent[F[_]: Concurrent, L: Monoid]: Concurrent[WriterT[F, L, ?]] =
        new WriterTConcurrent[F, L]
        { def F = Concurrent [F]; def L = Monoid [L] }

        private [effect] trait EitherTConcurrent[F[_], L] extends Async.EitherTAsync[F, L]
        with Concurrent[EitherT[F, L, ?]]
        {

            override protected implicit def F: Concurrent[F]
            override protected def FF = F

                // Needed to drive static checks, otherwise the
                // compiler will choke on type inference :-(
                type Fiber [A] = cats.effect.Fiber[EitherT[F, L, ?], A]

            override def cancelable[A](k:(Either[Throwable, A] => Unit) => CancelToken[EitherT[F, L, ?]]): EitherT[F, L, A] =
            EitherT.liftF(F.cancelable(k.andThen(_.value.map(_ =>()))))(F)

            override def start[A](fa: EitherT[ F, L, A]) =
            EitherT.liftF(F.start(fa.value).map(fiberT))

            override def racePair[A, B](fa: EitherT[ F, L, A], fb: EitherT[F, L, B]): EitherT[F, L, Either[(A, Fiber[B]), (Fiber[A], B)]] =
            EitherT(F.racePair(fa.value, fb.value).flatMap {
                case Left ((value, fiberB)) =>
                value match {
                    case Left (_) =>
                    fiberB.cancel.map(_ => value . asInstanceOf [Left[L, Nothing]])
                    case Right (r) =>
                    F.pure(Right(Left((r, fiberT[B](fiberB)))))
                }
                case Right ((fiberA, value)) =>
                value match {
                    case Left (_) =>
                    fiberA.cancel.map(_ => value . asInstanceOf [Left[L, Nothing]])
                    case Right (r) =>
                    F.pure(Right(Right((fiberT[A](fiberA), r))))
                }
            })

            protected def fiberT[A](fiber: effect. Fiber [F, Either[L, A]]): Fiber[A] =
            Fiber(EitherT(fiber.join), EitherT.liftF(fiber.cancel))
        }

        private [effect] trait OptionTConcurrent[F[_]] extends Async.OptionTAsync[F]
        with Concurrent[OptionT[F, ?]]
        {

            override protected implicit def F: Concurrent[F]
            override protected def FF = F

                // Needed to drive static checks, otherwise the
                // compiler will choke on type inference :-(
                type Fiber [A] = cats.effect.Fiber[OptionT[F, ?], A]

            override def cancelable[A](k:(Either[Throwable, A] => Unit) => CancelToken[OptionT[F, ?]]): OptionT[F, A] =
            OptionT.liftF(F.cancelable(k.andThen(_.value.map(_ =>()))))(F)

            override def start[A](fa: OptionT[ F, A]) =
            OptionT.liftF(F.start(fa.value).map(fiberT))

            override def racePair[A, B](fa: OptionT[ F, A], fb: OptionT[F, B]): OptionT[F, Either[(A, Fiber[B]), (Fiber[A], B)]] =
            OptionT(F.racePair(fa.value, fb.value).flatMap {
                case Left ((value, fiberB)) =>
                value match {
                    case None =>
                    fiberB.cancel.map(_ => None)
                    case Some (r) =>
                    F.pure(Some(Left((r, fiberT[B](fiberB)))))
                }
                case Right ((fiberA, value)) =>
                value match {
                    case None =>
                    fiberA.cancel.map(_ => None)
                    case Some (r) =>
                    F.pure(Some(Right((fiberT[A](fiberA), r))))
                }
            })

            protected def fiberT[A](fiber: effect. Fiber [F, Option[A]]): Fiber[A] =
            Fiber(OptionT(fiber.join), OptionT.liftF(fiber.cancel))
        }


        private [effect] trait WriterTConcurrent[F[_], L] extends Async.WriterTAsync[F, L]
        with Concurrent[WriterT[F, L, ?]]
        {

            override protected implicit def F: Concurrent[F]
            override protected def FA = F

                // Needed to drive static checks, otherwise the
                // compiler will choke on type inference :-(
                type Fiber [A] = cats.effect.Fiber[WriterT[F, L, ?], A]

            override def cancelable[A](k:(Either[Throwable, A] => Unit) => CancelToken[WriterT[F, L, ?]]): WriterT[F, L, A] =
            WriterT.liftF(F.cancelable(k.andThen(_.run.map(_ =>()))))(L, F)

            override def start[A](fa: WriterT[ F, L, A]) =
            WriterT(F.start(fa.run).map {
                fiber =>
                (L.empty, fiberT[A](fiber))
            })

            override def racePair[A, B](fa: WriterT[ F, L, A], fb: WriterT[F, L, B]): WriterT[F, L, Either[(A, Fiber[B]), (Fiber[A], B)]] =
            WriterT(F.racePair(fa.run, fb.run).map {
                case Left (((l, value), fiber)) =>
                (l, Left((value, fiberT(fiber))))
                case Right ((fiber, (l, value))) =>
                (l, Right((fiberT(fiber), value)))
            })

            protected def fiberT[A](fiber: effect. Fiber [F, (L, A)]): Fiber[A] =
            Fiber(WriterT(fiber.join), WriterT.liftF(fiber.cancel))
        }

        private [effect]
        abstract class KleisliConcurrent[F[_], R]
        extends Async.KleisliAsync[F, R]
        with Concurrent[Kleisli[F, R, ?]]
        {

            override protected implicit def F: Concurrent[F]
            // Needed to drive static checks, otherwise the
            // compiler can choke on type inference :-(
            type Fiber [A] = cats.effect.Fiber[Kleisli[F, R, ?], A]

            override def cancelable[A](k:(Either[Throwable, A] => Unit) => CancelToken[Kleisli[F, R, ?]]): Kleisli[F, R, A] =
            Kleisli(r => F . cancelable (k.andThen(_.run(r).map(_ =>()))))

            override def start[A](fa: Kleisli[ F, R, A]): Kleisli[F, R, Fiber[A]] =
            Kleisli(r => F . start (fa.run(r)).map(fiberT))

            override def racePair[A, B](fa: Kleisli[ F, R, A], fb: Kleisli[F, R, B]) =
            Kleisli {
                r =>
                F.racePair(fa.run(r), fb.run(r)).map {
                    case Left ((a, fiber)) => Left((a, fiberT[B](fiber)))
                    case Right ((fiber, b)) => Right((fiberT[A](fiber), b))
                }
            }

            protected def fiberT[A](fiber: effect. Fiber [F, A]): Fiber[A] =
            Fiber(Kleisli.liftF(fiber.join), Kleisli.liftF(fiber.cancel))
        }

        */
        /**
         * Internal API — Cancelable builder derived from
         * [[Async.asyncF]] and [[Bracket.bracketCase]].
         *//*

        private def defaultCancelable[F[_], A](k: (Either[Throwable, A] => Unit) => CancelToken[F])
        (implicit F: Async[F]): F[A] =
        {

            F.asyncF[A] {
                cb =>
                // For back-pressuring bracketCase until the callback gets called.
                // Need to work with `Promise` due to the callback being side-effecting.
                val latch = Promise[Unit]()
                val latchF = F.async[Unit](cb => latch . future . onComplete (_ => cb(rightUnit))(immediate))
                // Side-effecting call; unfreezes latch in order to allow bracket to finish
                val token = k {
                    result =>
                    latch.complete(successUnit)
                    cb(result)
                }
                F.bracketCase(F.pure(token))(_ => latchF) {
                case(cancel, Canceled) => cancel
                case _ => F . unit
            }
            }
        }
    }
}
*/
