package arrow.concurrent

import kollect.arrow.concurrent.Duration
import java.util.concurrent.TimeoutException

/**
 * An object that may eventually be completed with a result value of type `T` which may be
 * awaited using blocking methods.
 *
 * The [[Await]] object provides methods that allow accessing the result of an `Awaitable`
 * by blocking the current thread until the `Awaitable` has been completed or a timeout has
 * occurred.
 */
interface Awaitable<out T> {

    /**
     * Await the "completed" state of this `Awaitable`.
     *
     * '''''This method should not be called directly; use [[Await.ready]] instead.'''''
     *
     * @param atMost
     *         maximum wait time, which may be negative (no waiting is done),
     *         [[scala.concurrent.duration.Duration.Inf Duration.Inf]] for unbounded waiting, or a finite positive
     *         duration
     * @return this `Awaitable`
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TimeoutException if after waiting for the specified time this `Awaitable` is still not ready
     * @throws IllegalArgumentException if `atMost` is [[scala.concurrent.duration.Duration.Undefined Duration.Undefined]]
     */
    @Throws(TimeoutException::class, InterruptedException::class)
    fun ready(permit: CanAwait, atMost: Duration): Awaitable<T>

    /**
     * Await and return the result (of type `T`) of this `Awaitable`.
     *
     * '''''This method should not be called directly; use [[Await.result]] instead.'''''
     *
     * @param atMost
     *         maximum wait time, which may be negative (no waiting is done),
     *         [[scala.concurrent.duration.Duration.Inf Duration.Inf]] for unbounded waiting, or a finite positive
     *         duration
     * @return the result value if the `Awaitable` is completed within the specific maximum wait time
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TimeoutException if after waiting for the specified time this `Awaitable` is still not ready
     * @throws IllegalArgumentException if `atMost` is [[scala.concurrent.duration.Duration.Undefined Duration.Undefined]]
     */
    @Throws(Exception::class)
    fun result(permit: CanAwait, atMost: Duration): T
}
