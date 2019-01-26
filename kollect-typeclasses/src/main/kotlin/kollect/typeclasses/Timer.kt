package kollect.typeclasses

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
interface Timer<F> {
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
    fun sleep(duration: FiniteDuration): arrow.Kind<F, Unit>

    companion object
}
