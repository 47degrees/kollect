package kollect.arrow

import arrow.concurrent.Awaitable
import arrow.concurrent.CanAwait
import arrow.concurrent.Promise
import arrow.core.Failure
import arrow.core.None
import arrow.core.Option
import arrow.core.PartialFunction
import arrow.core.Some
import arrow.core.Success
import arrow.core.Try
import arrow.core.Tuple2
import kollect.ControlThrowable
import kollect.NonLocalReturnControl
import kollect.arrow.PromiseImpl.Companion.resolveTry
import kollect.arrow.concurrent.Duration
import kollect.arrow.concurrent.FiniteDuration
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.AbstractQueuedSynchronizer

interface PromiseImpl<T> : Promise<T>, Future<T> {
    override fun future(): Future<T> = this

    override fun <S> transform(executor: ExecutionContext, f: (Try<T>) -> Try<S>): Future<S> {
        val p = DefaultPromise<S>()
        onComplete(executor) { result ->
            p.complete(try {
                f(result)
            } catch (nonFatal: Exception) {
                Failure(nonFatal)
            })
        }
        return p.future()
    }

    // If possible, link DefaultPromises to avoid space leaks
    override fun <S> transformWith(executor: ExecutionContext, f: (Try<T>) -> Future<S>): Future<S> {
        val p = DefaultPromise<S>()
        onComplete(executor) { v ->
            try {
                val res = f(v)
                when {
                    res == this -> p.complete(v as Try<S>)
                    res is DefaultPromise<*> -> (res as DefaultPromise<S>).linkRootOf(p)
                    else -> p.completeWith(res)
                }
            } catch (nonFatal: Exception) {
                p.failure(nonFatal)
            }
        }
        return p.future()
    }

    companion object {

        fun <T> resolveTry(source: Try<T>): Try<T> = when (source) {
            is Failure -> resolver(source.exception)
            else -> source
        }

        private fun <T> resolver(throwable: Throwable): Try<T> = when (throwable) {
            is NonLocalReturnControl -> Success(throwable as T)
            is ControlThrowable -> Failure(ExecutionException("Boxed ControlThrowable", throwable))
            is InterruptedException -> Failure(ExecutionException("Boxed InterruptedException", throwable))
            is Error -> Failure(ExecutionException("Boxed Error", throwable))
            else -> Failure(throwable)
        }

        class CompletionLatch<T> : AbstractQueuedSynchronizer(), (Try<T>) -> Unit {
            override fun tryAcquireShared(arg: Int): Int = if (state != 0) 1 else -1

            override fun tryReleaseShared(arg: Int): Boolean {
                state = 1
                return true
            }

            override fun invoke(p1: Try<T>): Unit {
                releaseShared(1)
            }
        }
    }
}

class DefaultPromise<T> : AtomicReference<Any>(null), PromiseImpl<T> {

    /** Get the root promise for this promise, compressing the link chain to that
     *  promise if necessary.
     *
     *  For promises that are not linked, the result of calling `compressedRoot()` will the promise itself. However for
     *  linked promises, this method will traverse each link until it locates the root promise at the base of the link
     *  chain.
     *
     *  As a side effect of calling this method, the link from this promise back to the root promise will be updated
     *  ("compressed") to point directly to the root promise. This allows intermediate promises in the link chain to be
     *  garbage collected. Also, subsequent calls to this method should be faster as the link chain will be shorter.
     */
    private fun compressedRoot(): DefaultPromise<T> = get().let {
        when (it) {
            is DefaultPromise<*> -> compressedRoot(it)
            else -> this
        }
    }

    private tailrec fun compressedRoot(linked: DefaultPromise<*>): DefaultPromise<T> {
        val target = (linked as DefaultPromise<T>).root()
        return if (linked == target) {
            target
        } else if (compareAndSet(linked, target)) {
            target
        } else {
            val value = get()
            when (value) {
                is DefaultPromise<*> -> compressedRoot(value)
                else -> this
            }
        }
    }

    /** Get the promise at the root of the chain of linked promises. Used by `compressedRoot()`.
     *  The `compressedRoot()` method should be called instead of this method, as it is important
     *  to compress the link chain whenever possible.
     */
    private fun root(): DefaultPromise<T> = get().let {
        when (it) {
            is DefaultPromise<*> -> (it as DefaultPromise<T>).root()
            else -> this
        }
    }

    /** Try waiting for this promise to be completed.
     */
    fun tryAwait(atMost: Duration): Boolean = if (!isCompleted()) {
        when (atMost) {
            is Duration.Companion.Infinite.Undefined -> throw IllegalArgumentException("cannot wait for Undefined period")
            is Duration.Companion.Infinite.Inf -> {
                val l = PromiseImpl.Companion.CompletionLatch<T>()
                onComplete(Future.Companion.InternalCallbackExecutor, l)
                l.acquireSharedInterruptibly(1)
            }
            is Duration.Companion.Infinite.MinusInf -> { /* Drop out */
            }
            is FiniteDuration -> if (atMost > Duration.Zero) {
                val l = PromiseImpl.Companion.CompletionLatch<T>()
                onComplete(Future.Companion.InternalCallbackExecutor, l)
                l.tryAcquireSharedNanos(1, atMost.toNanos())
            }
        }

        isCompleted()
    } else true // Already completed

    @Throws(TimeoutException::class, InterruptedException::class)
    override fun ready(permit: CanAwait, atMost: Duration): DefaultPromise<T> = if (tryAwait(atMost)) {
        this
    } else {
        throw TimeoutException("Futures timed out after [$atMost]")
    }

    @Throws(Exception::class)
    override fun result(permit: CanAwait, atMost: Duration): T =
        ready(permit, atMost).value().get().get() // ready throws TimeoutException if timeout so value.get is safe here

    override fun value(): Option<Try<T>> = value0()

    private fun value0(): Option<Try<T>> = get().let {
        when (it) {
            is Try<*> -> Some(it as Try<T>)
            is DefaultPromise<*> -> compressedRoot(it).value0()
            else -> None
        }
    }

    override fun isCompleted(): Boolean = isCompleted0()

    private fun isCompleted0(): Boolean = get().let {
        when (it) {
            is Try<*> -> true
            is DefaultPromise<*> -> compressedRoot(it).isCompleted0()
            else -> false
        }
    }

    override fun tryComplete(result: Try<T>): Boolean {
        val resolved = resolveTry(result)
        val completeAndGetListeners = tryCompleteAndGetListeners(resolved)
        return when {
            completeAndGetListeners == null -> false
            completeAndGetListeners.isEmpty() -> true
            else -> true.also { completeAndGetListeners.forEach { r -> r.executeWithValue(resolved) } }
        }
    }

    /** Called by `tryComplete` to store the resolved value and get the list of
     *  listeners, or `null` if it is already completed.
     */
    private fun tryCompleteAndGetListeners(v: Try<T>): List<CallbackRunnable<T>>? =
        get().let {
            when (it) {
                is List<*> -> (it as List<CallbackRunnable<T>>).let {
                    if (compareAndSet(it, v)) it else tryCompleteAndGetListeners(v)
                }
                is DefaultPromise<*> -> compressedRoot(it).tryCompleteAndGetListeners(v)
                else -> null
            }
        }

    override fun <U : Any> onComplete(executor: ExecutionContext, func: (Try<T>) -> U): Unit =
        dispatchOrAddCallback(CallbackRunnable<T>(executor.prepare(), func))

    /** Tries to add the callback, if already completed, it dispatches the callback to be executed.
     *  Used by `onComplete()` to add callbacks to a promise and by `link()` to transfer callbacks
     *  to the root promise when linking two promises together.
     */
    private fun dispatchOrAddCallback(runnable: CallbackRunnable<T>): Unit =
        get().let {
            when (it) {
                is Try<*> -> runnable.executeWithValue(it as Try<T>)
                is DefaultPromise<*> -> compressedRoot(it).dispatchOrAddCallback(runnable)
                is List<*> -> if (compareAndSet(it, listOf(runnable) + it)) else dispatchOrAddCallback(runnable)
            }
        }

    /** Link this promise to the root of another promise using `link()`. Should only be
     *  be called by transformWith.
     */
    fun linkRootOf(target: DefaultPromise<T>): Unit = link(target.compressedRoot())

    /** Link this promise to another promise so that both promises share the same
     *  externally-visible state. Depending on the current state of this promise, this
     *  may involve different things. For example, any onComplete listeners will need
     *  to be transferred.
     *
     *  If this promise is already completed, then the same effect as linking -
     *  sharing the same completed value - is achieved by simply sending this
     *  promise's result to the target promise.
     */
    private fun link(target: DefaultPromise<T>): Unit {
        if (this != target) {
            get().let { value ->
                when {
                    value is Try<*> -> if (!target.tryComplete(value as Try<T>)) {
                        throw IllegalStateException("Cannot link completed promises together")
                    }
                    value is DefaultPromise<*> -> compressedRoot(value).link(target)
                    value is List<*> && compareAndSet(value, target) -> if (value.isNotEmpty())
                        (value as List<CallbackRunnable<T>>).forEach { target.dispatchOrAddCallback(it) }
                    else -> link(target)
                }
            }
        }
    }
}

object KeptPromise {

    private sealed class Kept<T> : PromiseImpl<T> {

        abstract fun result(): Try<T>

        override fun value(): Option<Try<T>> = Some(result())

        override fun isCompleted(): Boolean = true

        override fun tryComplete(result: Try<T>): Boolean = false

        override fun <U : Any> onComplete(executor: ExecutionContext, f: (Try<T>) -> U): Unit =
            (CallbackRunnable(executor.prepare(), f)).executeWithValue(result())

        override fun ready(permit: CanAwait, atMost: Duration): Awaitable<T> = this

        override fun result(permit: CanAwait, atMost: Duration): T = result().get()

        class Successful<T>(val result: Success<T>) : Kept<T>() {
            override fun result(): Try<T> = result

            override fun <U : Any> onFailure(executor: ExecutionContext, pf: PartialFunction<Throwable, U>) = Unit
            override fun failed(executor: ExecutionContext): Future<Throwable> = KeptPromise(Failure(NoSuchElementException("Future.failed not completed with a throwable."))).future()

            // override def recover[U >: T](pf: PartialFunction[Throwable, U])(implicit executor: ExecutionContext): Future[U] = this
            // override def recoverWith[U >: T](pf: PartialFunction[Throwable, Future[U]])(implicit executor: ExecutionContext): Future[U] = this
            // override def fallbackTo[U >: T](that: Future[U]): Future[U] = this
        }

        class Failed<T>(val result: Failure) : Kept<T>() {

            override fun result(): Try<T> = result

            private fun <S> thisAs(): Future<S> = future() as Future<S>

            override fun <U : Any> onSuccess(executor: ExecutionContext, pf: PartialFunction<T, U>) = Unit
            override fun failed(executor: ExecutionContext): Future<Throwable> = KeptPromise(Success(result.exception)).future()
            override fun <U> foreach(executor: ExecutionContext, f: (T) -> U) = Unit
            override fun <S> map(executor: ExecutionContext, f: (T) -> S): Future<S> = thisAs()
            override fun <S> flatMap(executor: ExecutionContext, f: (T) -> Future<S>): Future<S> = thisAs()
            override fun <S> flatten(ev: (T) -> Future<S>): Future<S> = thisAs()
            override fun filter(executor: ExecutionContext, p: (T) -> Boolean): Future<T> = this
            override fun <S> collect(executor: ExecutionContext, pf: PartialFunction<T, S>): Future<S> = thisAs()
            override fun <U> zip(that: Future<U>): Future<Tuple2<T, U>> = thisAs()
            override fun <U, R> zipWith(that: Future<U>, f: (T, U) -> R): Future<R> = thisAs()
            // override def fallbackTo[U >: T](that: Future[U]): Future[U] =
            //      if (this eq that) this else that.recoverWith({ case _ => this })(InternalCallbackExecutor)
            // override def mapTo[S](implicit tag: ClassTag[S]): Future[S] = thisAs[S]
        }
    }
    
    operator fun <T> invoke(result: Try<T>): Promise<T> {
        val resolvedTry = PromiseImpl.resolveTry(result)
        return when (resolvedTry) {
            is Success -> Kept.Successful(resolvedTry)
            is Failure -> Kept.Failed(resolvedTry)
        }
    }
}

private class CallbackRunnable<T>(val executor: ExecutionContext, val onComplete: (Try<T>) -> Any) : Runnable, OnCompleteRunnable {
    // must be filled in before running it
    var value: Try<T>? = null

    override fun run(): Unit {
        if (value != null) {
            try {
                onComplete(value!!)
            } catch (nonFatal: Exception) {
                executor.reportFailure(nonFatal)
            }
        }
    }

    fun executeWithValue(v: Try<T>): Unit {
        if (value == null) {
            value = v
            // Note that we cannot prepare the ExecutionContext at this point, since we might
            // already be running on a different thread!
            try {
                executor.execute(this)
            } catch (nonFatal: Exception) {
                executor.reportFailure(nonFatal)
            }
        }
    }
}

interface OnCompleteRunnable : Runnable