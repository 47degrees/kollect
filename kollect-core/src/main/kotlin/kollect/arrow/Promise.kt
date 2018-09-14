package arrow.concurrent

import arrow.core.Failure
import arrow.core.Success
import arrow.core.Try
import kollect.arrow.DefaultPromise
import kollect.arrow.Future

/** Promise is an object which can be completed with a value or failed
 *  with an exception.
 *
 *  @define promiseCompletion
 *  If the promise has already been fulfilled, failed or has timed out,
 *  calling this method will throw an IllegalStateException.
 *
 *  @define allowedThrowables
 *  If the throwable used to fail this promise is an error, a control exception
 *  or an interrupted exception, it will be wrapped as a cause within an
 *  `ExecutionException` which will fail the promise.
 *
 *  @define nonDeterministic
 *  Note: Using this method may result in non-deterministic concurrent programs.
 */
interface Promise<T> {
    /** Future containing the value of this promise.
     */
    fun future(): Future<T>

    /** Returns whether the promise has already been completed with
     *  a value or an exception.
     *
     *  $nonDeterministic
     *
     *  @return    `true` if the promise is already completed, `false` otherwise
     */
    fun isCompleted(): Boolean

    /** Completes the promise with either an exception or a value.
     *
     *  @param result     Either the value or the exception to complete the promise with.
     *
     *  $promiseCompletion
     */
    fun complete(result: Try<T>): Promise<T> =
        if (tryComplete(result)) this else throw IllegalStateException("Promise already completed.")

    /** Tries to complete the promise with either a value or the exception.
     *
     *  $nonDeterministic
     *
     *  @return    If the promise has already been completed returns `false`, or `true` otherwise.
     */
    fun tryComplete(result: Try<T>): Boolean

    /** Completes this promise with the specified future, once that future is completed.
     *
     *  @return   This promise
     */
    fun completeWith(other: Future<T>): Promise<T> = tryCompleteWith(other)

    /** Attempts to complete this promise with the specified future, once that future is completed.
     *
     *  @return   This promise
     */
    fun tryCompleteWith(other: Future<T>): Promise<T> {
        if (other != this.future()) { // this tryCompleteWith this doesn't make much sense
            other.onComplete(Future.InternalCallbackExecutor) { tryComplete(it) }
        }
        return this
    }

    /** Completes the promise with a value.
     *
     *  @param value The value to complete the promise with.
     *
     *  $promiseCompletion
     */
    fun success(value: T): Promise<T> = complete(Success(value))

    /** Tries to complete the promise with a value.
     *
     *  $nonDeterministic
     *
     *  @return    If the promise has already been completed returns `false`, or `true` otherwise.
     */
    fun trySuccess(value: T): Boolean = tryComplete(Success(value))

    /** Completes the promise with an exception.
     *
     *  @param cause    The throwable to complete the promise with.
     *
     *  $allowedThrowables
     *
     *  $promiseCompletion
     */
    fun failure(cause: Throwable): Promise<T> = complete(Failure(cause))

    /** Tries to complete the promise with an exception.
     *
     *  $nonDeterministic
     *
     *  @return    If the promise has already been completed returns `false`, or `true` otherwise.
     */
    fun tryFailure(cause: Throwable): Boolean = tryComplete(Failure(cause))

    companion object {
        /** Creates a promise object which can be completed with a value.
         *
         *  @tparam T       the type of the value in the promise
         *  @return         the newly created `Promise` object
         */
        operator fun <T> invoke(): Promise<T> = DefaultPromise()

        /** Creates an already completed Promise with the specified exception.
         *
         *  @tparam T       the type of the value in the promise
         *  @return         the newly created `Promise` object
         */
        fun <T> failed(exception: Throwable): Promise<T> = fromTry(Failure(exception))

        /** Creates an already completed Promise with the specified result.
         *
         *  @tparam T       the type of the value in the promise
         *  @return         the newly created `Promise` object
         */
        fun <T> successful(result: T): Promise<T> = fromTry(Success(result))

        /** Creates an already completed Promise with the specified result or exception.
         *
         *  @tparam T       the type of the value in the promise
         *  @return         the newly created `Promise` object
         */
        fun <T> fromTry(result: Try<T>): Promise<T> = impl.Promise.KeptPromise<T>(result)
    }
}

