package kollect.arrow

import kollect.arrow.concurrent.FiniteDuration
import java.util.concurrent.TimeUnit

/**
 * This class stores a deadline, as obtained via `Deadline.now` or the
 * duration DSL:
 *
 * {{{
 * import scala.concurrent.duration._
 * 3.seconds.fromNow
 * }}}
 *
 * Its main purpose is to manage repeated attempts to achieve something (like
 * awaiting a condition) by offering the methods `hasTimeLeft` and `timeLeft`.  All
 * durations are measured according to `System.nanoTime` aka wall-time; this
 * does not take into account changes to the system clock (such as leap
 * seconds).
 */
data class Deadline(val time: FiniteDuration) {

    companion object {
        /**
         * Construct a deadline due exactly at the point where this method is called. Useful for then
         * advancing it to obtain a future deadline, or for sampling the current time exactly once and
         * then comparing it to multiple deadlines (using subtraction).
         */
        fun now(): Deadline = Deadline(kollect.arrow.concurrent.Duration(System.nanoTime(), TimeUnit.NANOSECONDS))

        /**
         * The natural ordering for deadline is determined by the natural order of the underlying (finite) duration.
         */
        val DeadlineIsOrdered = Comparator<Deadline> { o1, o2 -> o1.compare(o2) }
    }

    /**
     * Return a deadline advanced (i.e., moved into the future) by the given duration.
     */
    operator fun plus(other: FiniteDuration): Deadline = copy(time = time + other)

    /**
     * Return a deadline moved backwards (i.e., towards the past) by the given duration.
     */
    operator fun minus(other: FiniteDuration): Deadline = copy(time = time - other)

    /**
     * Calculate time difference between this and the other deadline, where the result is directed (i.e., may be negative).
     */
    operator fun minus(other: Deadline): FiniteDuration = time - other.time

    /**
     * Calculate time difference between this duration and now; the result is negative if the deadline has passed.
     *
     * '''''Note that on some systems this operation is costly because it entails a system call.'''''
     * Check `System.nanoTime` for your platform.
     */
    fun timeLeft(): FiniteDuration = this - Deadline.now()

    /**
     * Determine whether the deadline still lies in the future at the point where this method is called.
     *
     * '''''Note that on some systems this operation is costly because it entails a system call.'''''
     * Check `System.nanoTime` for your platform.
     */
    fun hasTimeLeft(): Boolean = !isOverdue()

    /**
     * Determine whether the deadline lies in the past at the point where this method is called.
     *
     * '''''Note that on some systems this operation is costly because it entails a system call.'''''
     * Check `System.nanoTime` for your platform.
     */
    fun isOverdue(): Boolean = (time.toNanos() - System.nanoTime()) < 0

    /**
     * The natural ordering for deadline is determined by the natural order of the underlying (finite) duration.
     */
    fun compare(other: Deadline) = time.compareTo(other.time)
}
