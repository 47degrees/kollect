package kollect.arrow


/**
 * Timer is a scheduler of tasks.
 *
 * This is the purely functional equivalent of:
 *
 *  - Java's
 *    [[https://docs.oracle.com/javase/9/docs/api/java/util/concurrent/ScheduledExecutorService.html ScheduledExecutorService]]
 *  - JavaScript's
 *    [[https://developer.mozilla.org/en-US/docs/Web/API/WindowOrWorkerGlobalScope/setTimeout setTimeout]].
 *
 * It provides:
 *
 *  1. the ability to get the current time
 *  1. ability to delay the execution of a task with a specified time duration
 *
 * It does all of that in an `F` monadic context that can suspend
 * side effects and is capable of asynchronous execution (e.g. [[IO]]).
 *
 * This is NOT a type class, as it does not have the coherence
 * requirement.
 */
interface Timer<F>  {
    /**
     * Returns a [[Clock]] instance associated with this timer
     * that can provide the current time and do time measurements.
     */
    fun clock(): Clock<F>

    /**
     * Creates a new task that will sleep for the given duration,
     * emitting a tick when that time span is over.
     *
     * As an example on evaluation this will print "Hello!" after
     * 3 seconds:
     *
     * {{{
     *   import cats.effect._
     *   import scala.concurrent.duration._
     *
     *   Timer[IO].sleep(3.seconds).flatMap { _ =>
     *     IO(println("Hello!"))
     *   }
     * }}}
     *
     * Note that `sleep` is required to introduce an asynchronous
     * boundary, even if the provided `timespan` is less or
     * equal to zero.
     */
    def sleep(duration: FiniteDuration): F[Unit]
}

object Timer {
    /**
     * Derives a [[Timer]] instance for `cats.data.EitherT`,
     * given we have one for `F[_]`.
     */
    implicit def deriveEitherT[F[_], L](implicit F: Functor[F], timer: Timer[F]): Timer[EitherT[F, L, ?]] =
    new Timer[EitherT[F, L, ?]] {
        val clock: Clock[EitherT[F, L, ?]] = Clock.deriveEitherT

        def sleep(duration: FiniteDuration): EitherT[F, L, Unit] =
        EitherT.liftF(timer.sleep(duration))
    }

    /**
     * Derives a [[Timer]] instance for `cats.data.OptionT`,
     * given we have one for `F[_]`.
     */
    implicit def deriveOptionT[F[_]](implicit F: Functor[F], timer: Timer[F]): Timer[OptionT[F, ?]] =
    new Timer[OptionT[F, ?]] {
        val clock: Clock[OptionT[F, ?]] = Clock.deriveOptionT

        def sleep(duration: FiniteDuration): OptionT[F, Unit] =
        OptionT.liftF(timer.sleep(duration))
    }

    /**
     * Derives a [[Timer]] instance for `cats.data.WriterT`,
     * given we have one for `F[_]`.
     */
    implicit def deriveWriterT[F[_], L](implicit F: Applicative[F], L: Monoid[L], timer: Timer[F]): Timer[WriterT[F, L, ?]] =
    new Timer[WriterT[F, L, ?]] {
        val clock: Clock[WriterT[F, L, ?]] = Clock.deriveWriterT

        def sleep(duration: FiniteDuration): WriterT[F, L, Unit] =
        WriterT.liftF(timer.sleep(duration))
    }

    /**
     * Derives a [[Timer]] instance for `cats.data.StateT`,
     * given we have one for `F[_]`.
     */
    implicit def deriveStateT[F[_], S](implicit F: Applicative[F], timer: Timer[F]): Timer[StateT[F, S, ?]] =
    new Timer[StateT[F, S, ?]] {
        val clock: Clock[StateT[F, S, ?]] = Clock.deriveStateT

        def sleep(duration: FiniteDuration): StateT[F, S, Unit] =
        StateT.liftF(timer.sleep(duration))
    }

    /**
     * Derives a [[Timer]] instance for `cats.data.Kleisli`,
     * given we have one for `F[_]`.
     */
    implicit def deriveKleisli[F[_], R](implicit timer: Timer[F]): Timer[Kleisli[F, R, ?]] =
    new Timer[Kleisli[F, R, ?]] {
        val clock: Clock[Kleisli[F, R, ?]] = Clock.deriveKleisli

        def sleep(duration: FiniteDuration): Kleisli[F, R, Unit] =
        Kleisli.liftF(timer.sleep(duration))
    }
}
