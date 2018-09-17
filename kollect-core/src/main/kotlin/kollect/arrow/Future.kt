package kollect.arrow

import arrow.concurrent.Awaitable
import arrow.concurrent.BatchingExecutor
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
import arrow.core.invokeOrElse
import kollect.arrow.concurrent.Duration
import kollect.arrow.concurrent.FiniteDuration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

interface Future<out T> : Awaitable<T> {

    companion object {

        private val toBoxed = mapOf(
            Boolean::class to java.lang.Boolean::class,
            Byte::class to java.lang.Byte::class,
            Char::class to java.lang.Character::class,
            Short::class to java.lang.Short::class,
            Int::class to java.lang.Integer::class,
            Long::class to java.lang.Long::class,
            Float::class to java.lang.Float::class,
            Double::class to java.lang.Double::class
        )

        /** A Future which is never completed.
         */
        object never : Future<Nothing> {

            @Throws(TimeoutException::class, InterruptedException::class)
            override fun ready(permit: CanAwait, atMost: Duration): Future<Nothing> {
                when (atMost) {
                    is Duration.Companion.Infinite.Undefined -> throw IllegalArgumentException("cannot wait for Undefined period")
                    is Duration.Companion.Infinite.Inf -> {
                        CountDownLatch(1).await()
                        TimeoutException("Future timed out after [$atMost]")
                    }
                    is Duration.Companion.Infinite.MinusInf -> {
                        /* Drop out */
                    }
                    is FiniteDuration -> if (atMost > Duration.Zero) {
                        CountDownLatch(1).await(atMost.toNanos(), TimeUnit.NANOSECONDS)
                        TimeoutException("Future timed out after [$atMost]")
                    } else {
                        TimeoutException("Future timed out after [$atMost]")
                    }
                }
                return this
            }

            @Throws(Exception::class)
            override fun result(permit: CanAwait, atMost: Duration): Nothing {
                ready(permit, atMost)
                throw TimeoutException("Future timed out after [$atMost]")
            }

            override fun <U : Any> onSuccess(executor: ExecutionContext, pf: PartialFunction<Nothing, U>): Unit = Unit
            override fun <U : Any> onFailure(executor: ExecutionContext, pf: PartialFunction<Throwable, U>) = Unit
            override fun <U : Any> onComplete(executor: ExecutionContext, f: (Try<Nothing>) -> U) = Unit
            override fun isCompleted(): Boolean = false
            override fun value(): Option<Try<Nothing>> = None
            override fun failed(executor: ExecutionContext): Future<Throwable> = this
            override fun <U> foreach(executor: ExecutionContext, f: (Nothing) -> U) = Unit
            override fun <S> transform(executor: ExecutionContext, f: (Try<Nothing>) -> Try<S>): Future<S> = this
            override fun <S> transform(executor: ExecutionContext, s: (Nothing) -> S, f: (Throwable) -> Throwable): Future<S> = this
            override fun <S> transformWith(executor: ExecutionContext, f: (Try<Nothing>) -> Future<S>): Future<S> = this
            override fun <S> map(executor: ExecutionContext, f: (Nothing) -> S): Future<S> = this
            override fun <S> flatMap(executor: ExecutionContext, f: (Nothing) -> Future<S>): Future<S> = this
            override fun <S> flatten(ev: (Nothing) -> Future<S>): Future<S> = this
            override fun filter(executor: ExecutionContext, p: (Nothing) -> Boolean): Future<Nothing> = this
            override fun <S> collect(executor: ExecutionContext, pf: PartialFunction<Nothing, S>): Future<S> = this
            // override fun <U, T : U> recover(executor: ExecutionContext, pf: PartialFunction<Throwable, U>): Future<U> = this
            // override fun <U, T : U> recoverWith(executor: ExecutionContext, pf: PartialFunction<Throwable, Future<U>>): Future<U> = this
            override fun <U> zip(that: Future<U>): Future<Tuple2<Nothing, U>> = this

            override fun <U, R> zipWith(that: Future<U>, f: (Nothing, U) -> R): Future<R> = this
            // override fun <U, T : U> fallbackTo(that: Future<U>): Future<U> = this
            // override fun <S> mapTo(tag: ClassTag<S>): Future<S> = this
            override fun <U> andThen(executor: ExecutionContext, pf: PartialFunction<Try<Nothing>, U>): Future<Nothing> = this

            override fun toString(): String = "Future(<never>)"
        }

        /** A Future which is always completed with the Unit value.
         */
        val unit: Future<Unit> = successful(Unit)

        /** Creates an already completed Future with the specified exception.
         *
         *  @tparam T        the type of the value in the future
         *  @param exception the non-null instance of `Throwable`
         *  @return          the newly created `Future` instance
         */
        fun <T> failed(exception: Throwable): Future<T> = Promise.failed<T>(exception).future()

        /** Creates an already completed Future with the specified result.
         *
         *  @tparam T       the type of the value in the future
         *  @param result   the given successful value
         *  @return         the newly created `Future` instance
         */
        fun <T> successful(result: T): Future<T> = Promise.successful(result).future()

        /** Creates an already completed Future with the specified result or exception.
         *
         *  @tparam T       the type of the value in the `Future`
         *  @param result   the result of the returned `Future` instance
         *  @return         the newly created `Future` instance
         */
        fun <T> fromTry(result: Try<T>): Future<T> = Promise.fromTry(result).future()

        /** Starts an asynchronous computation and returns a `Future` instance with the result of that computation.
         *
         *  The following expressions are equivalent:
         *
         *  {{{
         *  val f1 = Future(expr)
         *  val f2 = Future.unit.map(_ => expr)
         *  }}}
         *
         *  The result becomes available once the asynchronous computation is completed.
         *
         *  @tparam T        the type of the result
         *  @param body      the asynchronous computation
         *  @param executor  the execution context on which the future is run
         *  @return          the `Future` holding the result of the computation
         */
        fun <T> apply(executor: ExecutionContext, body: () -> T): Future<T> = unit.map(executor) { body() }

//        /** Simple version of `Future.traverse`. Asynchronously and non-blockingly transforms a `TraversableOnce[Future[A]]`
//         *  into a `Future[TraversableOnce[A]]`. Useful for reducing many `Future`s into a single `Future`.
//         *
//         * @tparam A        the type of the value inside the Futures
//         * @tparam M        the type of the `TraversableOnce` of Futures
//         * @param futures        the `TraversableOnce` of Futures which will be sequenced
//         * @return          the `Future` of the `TraversableOnce` of results
//         */
//        fun <A> sequence(executor: ExecutionContext, futures: List<Future<A>>): Future<List<A>> = {
//            futures.fold(successful(cbf(futures))) {
//                (fr, fa) -> fr.zipWith(fa)(_ += _)
//            }.map(_.result())(InternalCallbackExecutor)
//        }

        /** Asynchronously and non-blockingly returns a new `Future` to the result of the first future
         *  in the list that is completed. This means no matter if it is completed as a success or as a failure.
         *
         * @tparam T        the type of the value in the future
         * @param futures   the `TraversableOnce` of Futures in which to find the first completed
         * @return          the `Future` holding the result of the future that is first to be completed
         */
        fun <T> firstCompletedOf(futures: List<Future<T>>, executor: ExecutionContext): Future<T> {
            val p = Promise<T>()
            val firstCompleteHandler = object : AtomicReference<Promise<T>>(p), ((Try<T>) -> Unit) {
                override fun invoke(p1: Try<T>): Unit = getAndSet(null).let { promise ->
                    when (promise) {
                        null -> Unit
                        is Some<*> -> promise.tryComplete(p1)
                    }
                }
            }
            futures.forEach { it.onComplete(executor, firstCompleteHandler) }
            return p.future()
        }

        /** Asynchronously and non-blockingly returns a `Future` that will hold the optional result
         *  of the first `Future` with a result that matches the predicate, failed `Future`s will be ignored.
         *
         * @tparam T        the type of the value in the future
         * @param futures   the `scala.collection.immutable.Iterable` of Futures to search
         * @param p         the predicate which indicates if it's a match
         * @return          the `Future` holding the optional result of the search
         */
        fun <T> find(executor: ExecutionContext, futures: Iterable<Future<T>>, p: (T) -> Boolean): Future<Option<T>> {
            fun searchNext(i: Iterator<Future<T>>): Future<Option<T>> =
                if (!i.hasNext()) {
                    successful<Option<T>>(None)
                } else {
                    i.next().transformWith(executor) {
                        when {
                            it is Success && p(it.value) -> successful(Some(it.value))
                            else -> searchNext(i)
                        }
                    }
                }
            return searchNext(futures.iterator())
        }

        /** A non-blocking, asynchronous left fold over the specified futures,
         *  with the start value of the given zero.
         *  The fold is performed asynchronously in left-to-right order as the futures become completed.
         *  The result will be the first failure of any of the futures, or any failure in the actual fold,
         *  or the result of the fold.
         *
         *  Example:
         *  {{{
         *    val futureSum = Future.foldLeft(futures)(0)(_ + _)
         *  }}}
         *
         * @tparam T       the type of the value of the input Futures
         * @tparam R       the type of the value of the returned `Future`
         * @param futures  the `scala.collection.immutable.Iterable` of Futures to be folded
         * @param zero     the start value of the fold
         * @param op       the fold operation to be applied to the zero and futures
         * @return         the `Future` holding the result of the fold
         */
        fun <T, R> foldLeft(executor: ExecutionContext, futures: Iterable<Future<T>>, zero: R, op: (R, T) -> R): Future<R> =
            foldNext(executor, futures.iterator(), zero, op)

        private fun <T, R> foldNext(executor: ExecutionContext, i: Iterator<Future<T>>, prevValue: R, op: (R, T) -> R): Future<R> =
            if (!i.hasNext()) {
                successful(prevValue)
            } else {
                i.next().flatMap(executor) { value -> foldNext(executor, i, op(prevValue, value), op) }
            }


//        /** A non-blocking, asynchronous fold over the specified futures, with the start value of the given zero.
//         *  The fold is performed on the thread where the last future is completed,
//         *  the result will be the first failure of any of the futures, or any failure in the actual fold,
//         *  or the result of the fold.
//         *
//         *  Example:
//         *  {{{
//         *    val futureSum = Future.fold(futures)(0)(_ + _)
//         *  }}}
//         *
//         * @tparam T       the type of the value of the input Futures
//         * @tparam R       the type of the value of the returned `Future`
//         * @param futures  the `TraversableOnce` of Futures to be folded
//         * @param zero     the start value of the fold
//         * @param op       the fold operation to be applied to the zero and futures
//         * @return         the `Future` holding the result of the fold
//         */
//        @Deprecated("use Future.foldLeft instead")
//        fun <T, R> fold(executor: ExecutionContext, futures: List<Future<T>>, zero: R, op: (R, T) -> R): Future<R> =
//            if (futures.isEmpty()) {
//                successful(zero)
//            } else {
//                sequence(futures).map(_.foldLeft(zero)(op))
//            }

        /** Initiates a non-blocking, asynchronous, left reduction over the supplied futures
         *  where the zero is the result value of the first `Future`.
         *
         *  Example:
         *  {{{
         *    val futureSum = Future.reduceLeft(futures)(_ + _)
         *  }}}
         * @tparam T       the type of the value of the input Futures
         * @tparam R       the type of the value of the returned `Future`
         * @param futures  the `scala.collection.immutable.Iterable` of Futures to be reduced
         * @param op       the reduce operation which is applied to the results of the futures
         * @return         the `Future` holding the result of the reduce
         */
        fun <R, T : R> reduceLeft(executor: ExecutionContext, futures: Iterable<Future<T>>, op: (R, T) -> R): Future<R> {
            val i = futures.iterator()
            return if (!i.hasNext()) failed<T>(NoSuchElementException("reduceLeft attempted on empty collection"))
            else i.next().flatMap(executor) { v -> foldNext(executor, i, v, op) }
        }

//        /** Asynchronously and non-blockingly transforms a `TraversableOnce[A]` into a `Future[TraversableOnce[B]]`
//         *  using the provided function `A => Future[B]`.
//         *  This is useful for performing a parallel map. For example, to apply a function to all items of a list
//         *  in parallel:
//         *
//         *  {{{
//         *    val myFutureList = Future.traverse(myList)(x => Future(myFunc(x)))
//         *  }}}
//         * @tparam A        the type of the value inside the Futures in the `TraversableOnce`
//         * @tparam B        the type of the value of the returned `Future`
//         * @tparam M        the type of the `TraversableOnce` of Futures
//         * @param in        the `TraversableOnce` of Futures which will be sequenced
//         * @param fn        the function to apply to the `TraversableOnce` of Futures to produce the results
//         * @return          the `Future` of the `TraversableOnce` of results
//         */
//        def traverse[A, B, M[X] <: TraversableOnce[X]](in: M[A])(fn: A => Future[B])(implicit cbf: CanBuildFrom[M[A], B, M[B]], executor: ExecutionContext): Future[M[B]] =
//        in.foldLeft(successful(cbf(in))) {
//            (fr, a) => fr.zipWith(fn(a))(_ += _)
//        }.map(_.result())(InternalCallbackExecutor)

        object InternalCallbackExecutor : ExecutionContext, BatchingExecutor() {
            override fun unbatchedExecute(r: Runnable) = r.run()

            override fun reportFailure(cause: Throwable) =
                throw IllegalStateException("problem in scala.concurrent internal callback", cause)
        }
    }

    /* Callbacks */

    /** When this future is completed successfully (i.e., with a value),
     *  apply the provided partial function to the value if the partial function
     *  is defined at that value.
     *
     *  If the future has already been completed with a value,
     *  this will either be applied immediately or be scheduled asynchronously.
     *
     *  Note that the returned value of `pf` will be discarded.
     *
     *  $swallowsExceptions
     *  $multipleCallbacks
     *  $callbackInContext
     *
     * @group Callbacks
     */
    @Deprecated("use `foreach` or `onComplete` instead (keep in mind that they take total rather than partial functions)")
    fun <U : Any> onSuccess(executor: ExecutionContext, pf: PartialFunction<T, U>): Unit = onComplete(executor) {
        when (it) {
            is Success -> pf.invokeOrElse(it.value, it) // Exploiting the cached function to avoid MatchError
            else -> {
            }
        }
    }

    /** When this future is completed with a failure (i.e., with a throwable),
     *  apply the provided callback to the throwable.
     *
     *  $caughtThrowables
     *
     *  If the future has already been completed with a failure,
     *  this will either be applied immediately or be scheduled asynchronously.
     *
     *  Will not be called in case that the future is completed with a value.
     *
     *  Note that the returned value of `pf` will be discarded.
     *
     *  $swallowsExceptions
     *  $multipleCallbacks
     *  $callbackInContext
     *
     * @group Callbacks
     */
    @Deprecated("use `onComplete` or `failed.foreach` instead (keep in mind that they take total rather than partial functions)")
    fun <U : Any> onFailure(executor: ExecutionContext, pf: PartialFunction<Throwable, U>): Unit = onComplete(executor) {
        when (it) {
            is Failure -> pf.invokeOrElse(it.exception, it) // Exploiting the cached function to avoid MatchError
            else -> {
            }
        }
    }

    /** When this future is completed, either through an exception, or a value,
     *  apply the provided function.
     *
     *  If the future has already been completed,
     *  this will either be applied immediately or be scheduled asynchronously.
     *
     *  Note that the returned value of `f` will be discarded.
     *
     *  $swallowsExceptions
     *  $multipleCallbacks
     *  $callbackInContext
     *
     * @tparam U    only used to accept any return type of the given callback function
     * @param f     the function to be executed when this `Future` completes
     * @group Callbacks
     */
    fun <U : Any> onComplete(executor: ExecutionContext, f: (Try<T>) -> U): Unit

    /* Miscellaneous */

    /** Returns whether the future had already been completed with
     *  a value or an exception.
     *
     *  $nonDeterministic
     *
     *  @return    `true` if the future was completed, `false` otherwise
     * @group Polling
     */
    fun isCompleted(): Boolean

    /** The current value of this `Future`.
     *
     *  $nonDeterministic
     *
     *  If the future was not completed the returned value will be `None`.
     *  If the future was completed the value will be `Some(Success(t))`
     *  if it contained a valid result, or `Some(Failure(error))` if it contained
     *  an exception.
     *
     * @return    `None` if the `Future` wasn't completed, `Some` if it was.
     * @group Polling
     */
    fun value(): Option<Try<T>>

    /* Projections */

    /** The returned `Future` will be successfully completed with the `Throwable` of the original `Future`
     *  if the original `Future` fails.
     *
     *  If the original `Future` is successful, the returned `Future` is failed with a `NoSuchElementException`.
     *
     *  $caughtThrowables
     *
     * @return a failed projection of this `Future`.
     * @group Transformations
     */
    fun failed(executor: ExecutionContext): Future<Throwable> = transform(InternalCallbackExecutor) {
        when (it) {
            is Failure -> Success(it.exception)
            is Success -> Failure(NoSuchElementException("Future.failed not completed with a throwable."))
        }
    }

    /* Monadic operations */

    /** Asynchronously processes the value in the future once the value becomes available.
     *
     *  WARNING: Will not be called if this future is never completed or if it is completed with a failure.
     *
     *  $swallowsExceptions
     *
     * @tparam U     only used to accept any return type of the given callback function
     * @param f      the function which will be executed if this `Future` completes with a result,
     *               the return value of `f` will be discarded.
     * @group Callbacks
     */
    fun <U> foreach(executor: ExecutionContext, f: (T) -> U): Unit = onComplete(executor) { cb ->
        cb.fold({ Unit }) { f(it) }
    }

    /** Creates a new future by applying the 's' function to the successful result of
     *  this future, or the 'f' function to the failed result. If there is any non-fatal
     *  exception thrown when 's' or 'f' is applied, that exception will be propagated
     *  to the resulting future.
     *
     * @tparam S  the type of the returned `Future`
     * @param  s  function that transforms a successful result of the receiver into a successful result of the returned future
     * @param  f  function that transforms a failure of the receiver into a failure of the returned future
     * @return    a `Future` that will be completed with the transformed value
     * @group Transformations
     */
    fun <S> transform(executor: ExecutionContext, s: (T) -> S, f: (Throwable) -> Throwable): Future<S> =
        transform(executor) {
            when (it) {
                is Success -> Try { s(it.value) }
                is Failure -> Try { throw f(it.exception) } // will throw fatal errors!
            }
        }

    /** Creates a new Future by applying the specified function to the result
     * of this Future. If there is any non-fatal exception thrown when 'f'
     * is applied then that exception will be propagated to the resulting future.
     *
     * @tparam S  the type of the returned `Future`
     * @param  f  function that transforms the result of this future
     * @return    a `Future` that will be completed with the transformed value
     * @group Transformations
     */
    fun <S> transform(executor: ExecutionContext, f: (Try<T>) -> Try<S>): Future<S>

    /** Creates a new Future by applying the specified function, which produces a Future, to the result
     * of this Future. If there is any non-fatal exception thrown when 'f'
     * is applied then that exception will be propagated to the resulting future.
     *
     * @tparam S  the type of the returned `Future`
     * @param  f  function that transforms the result of this future
     * @return    a `Future` that will be completed with the transformed value
     * @group Transformations
     */
    fun <S> transformWith(executor: ExecutionContext, f: (Try<T>) -> Future<S>): Future<S>

    /** Creates a new future by applying a function to the successful result of
     *  this future. If this future is completed with an exception then the new
     *  future will also contain this exception.
     *
     *  Example:
     *
     *  {{{
     *  val f = Future { "The future" }
     *  val g = f map { x: String => x + " is now!" }
     *  }}}
     *
     *  Note that a for comprehension involving a `Future`
     *  may expand to include a call to `map` and or `flatMap`
     *  and `withFilter`.  See [[scala.concurrent.Future#flatMap]] for an example of such a comprehension.
     *
     *
     * @tparam S  the type of the returned `Future`
     * @param f   the function which will be applied to the successful result of this `Future`
     * @return    a `Future` which will be completed with the result of the application of the function
     * @group Transformations
     */
    fun <S> map(executor: ExecutionContext, f: (T) -> S): Future<S> = transform(executor) { it.map(f) }

    /** Creates a new future by applying a function to the successful result of
     *  this future, and returns the result of the function as the new future.
     *  If this future is completed with an exception then the new future will
     *  also contain this exception.
     *
     *  $forComprehensionExamples
     *
     * @tparam S  the type of the returned `Future`
     * @param f   the function which will be applied to the successful result of this `Future`
     * @return    a `Future` which will be completed with the result of the application of the function
     * @group Transformations
     */
    fun <S> flatMap(executor: ExecutionContext, f: (T) -> Future<S>): Future<S> = transformWith(executor) {
        when (it) {
            is Success -> f(it.value)
            is Failure -> this as Future<S>
        }
    }

    /** Creates a new future with one level of nesting flattened, this method is equivalent to `flatMap(identity)`.
     *
     * @tparam S  the type of the returned `Future`
     * @group Transformations
     */
    fun <S> flatten(ev: (T) -> Future<S>): Future<S> = flatMap(InternalCallbackExecutor, ev)

    /** Creates a new future by filtering the value of the current future with a predicate.
     *
     *  If the current future contains a value which satisfies the predicate, the new future will also hold that value.
     *  Otherwise, the resulting future will fail with a `NoSuchElementException`.
     *
     *  If the current future fails, then the resulting future also fails.
     *
     *  Example:
     *  {{{
     *  val f = Future { 5 }
     *  val g = f filter { _ % 2 == 1 }
     *  val h = f filter { _ % 2 == 0 }
     *  g foreach println // Eventually prints 5
     *  Await.result(h, Duration.Zero) // throw a NoSuchElementException
     *  }}}
     *
     * @param p   the predicate to apply to the successful result of this `Future`
     * @return    a `Future` which will hold the successful result of this `Future` if it matches the predicate or a `NoSuchElementException`
     * @group Transformations
     */
    fun filter(executor: ExecutionContext, p: (T) -> Boolean): Future<T> = map(executor) { r ->
        if (p(r)) r else throw NoSuchElementException("Future.filter predicate is not satisfied")
    }

    /** Used by for-comprehensions.
     * @group Transformations
     */
    fun withFilter(executor: ExecutionContext, p: (T) -> Boolean): Future<T> = filter(executor, p)

    /** Creates a new future by mapping the value of the current future, if the given partial function is defined at that value.
     *
     *  If the current future contains a value for which the partial function is defined, the new future will also hold that value.
     *  Otherwise, the resulting future will fail with a `NoSuchElementException`.
     *
     *  If the current future fails, then the resulting future also fails.
     *
     *  Example:
     *  {{{
     *  val f = Future { -5 }
     *  val g = f collect {
     *    case x if x < 0 => -x
     *  }
     *  val h = f collect {
     *    case x if x > 0 => x * 2
     *  }
     *  g foreach println // Eventually prints 5
     *  Await.result(h, Duration.Zero) // throw a NoSuchElementException
     *  }}}
     *
     * @tparam S    the type of the returned `Future`
     * @param pf    the `PartialFunction` to apply to the successful result of this `Future`
     * @return      a `Future` holding the result of application of the `PartialFunction` or a `NoSuchElementException`
     * @group Transformations
     */
    fun <S> collect(executor: ExecutionContext, pf: PartialFunction<T, S>): Future<S> = map(executor) { r ->
        pf.invokeOrElse(r) { t: T ->
            throw NoSuchElementException("Future.collect partial function is not defined at: $t")
        }
    }

//    /** Creates a new future that will handle any matching throwable that this
//     *  future might contain. If there is no match, or if this future contains
//     *  a valid result then the new future will contain the same.
//     *
//     *  Example:
//     *
//     *  {{{
//     *  Future (6 / 0) recover { case e: ArithmeticException => 0 } // result: 0
//     *  Future (6 / 0) recover { case e: NotFoundException   => 0 } // result: exception
//     *  Future (6 / 2) recover { case e: ArithmeticException => 0 } // result: 3
//     *  }}}
//     *
//     * @tparam U    the type of the returned `Future`
//     * @param pf    the `PartialFunction` to apply if this `Future` fails
//     * @return      a `Future` with the successful value of this `Future` or the result of the `PartialFunction`
//     * @group Transformations
//     */
//    fun <U, T : U> recover(executor: ExecutionContext, pf: PartialFunction<Throwable, U>): Future<U> =
//        transform(executor) { it.recover { e -> pf(e) } }

//    /** Creates a new future that will handle any matching throwable that this
//     *  future might contain by assigning it a value of another future.
//     *
//     *  If there is no match, or if this future contains
//     *  a valid result then the new future will contain the same result.
//     *
//     *  Example:
//     *
//     *  {{{
//     *  val f = Future { Int.MaxValue }
//     *  Future (6 / 0) recoverWith { case e: ArithmeticException => f } // result: Int.MaxValue
//     *  }}}
//     *
//     * @tparam U    the type of the returned `Future`
//     * @param pf    the `PartialFunction` to apply if this `Future` fails
//     * @return      a `Future` with the successful value of this `Future` or the outcome of the `Future` returned by the `PartialFunction`
//     * @group Transformations
//     */
//    fun <U, T : U> recoverWith(executor: ExecutionContext, pf: PartialFunction<Throwable, Future<U>>): Future<U> =
//        transformWith(executor) {
//            when (it) {
//                is Failure -> pf.invokeOrElse(it.exception) { _: Throwable -> this }
//                is Success -> this
//            }
//        }

    /** Zips the values of `this` and `that` future, and creates
     *  a new future holding the tuple of their results.
     *
     *  If `this` future fails, the resulting future is failed
     *  with the throwable stored in `this`.
     *  Otherwise, if `that` future fails, the resulting future is failed
     *  with the throwable stored in `that`.
     *
     * @tparam U      the type of the other `Future`
     * @param that    the other `Future`
     * @return        a `Future` with the results of both futures or the failure of the first of them that failed
     * @group Transformations
     */
    fun <U> zip(that: Future<U>): Future<Tuple2<T, U>> {
        return flatMap(InternalCallbackExecutor) { r1: T -> that.map(InternalCallbackExecutor) { r2: U -> Tuple2(r1, r2) } }
    }

    /** Zips the values of `this` and `that` future using a function `f`,
     *  and creates a new future holding the result.
     *
     *  If `this` future fails, the resulting future is failed
     *  with the throwable stored in `this`.
     *  Otherwise, if `that` future fails, the resulting future is failed
     *  with the throwable stored in `that`.
     *  If the application of `f` throws a throwable, the resulting future
     *  is failed with that throwable if it is non-fatal.
     *
     * @tparam U      the type of the other `Future`
     * @tparam R      the type of the resulting `Future`
     * @param that    the other `Future`
     * @param f       the function to apply to the results of `this` and `that`
     * @return        a `Future` with the result of the application of `f` to the results of `this` and `that`
     * @group Transformations
     */
    fun <U, R> zipWith(that: Future<U>, f: (T, U) -> R): Future<R> =
        flatMap(InternalCallbackExecutor) { r1 -> that.map(InternalCallbackExecutor) { r2 -> f(r1, r2) } }

//    /** Creates a new future which holds the result of this future if it was completed successfully, or, if not,
//     *  the result of the `that` future if `that` is completed successfully.
//     *  If both futures are failed, the resulting future holds the throwable object of the first future.
//     *
//     *  Using this method will not cause concurrent programs to become nondeterministic.
//     *
//     *  Example:
//     *  {{{
//     *  val f = Future { sys.error("failed") }
//     *  val g = Future { 5 }
//     *  val h = f fallbackTo g
//     *  h foreach println // Eventually prints 5
//     *  }}}
//     *
//     * @tparam U     the type of the other `Future` and the resulting `Future`
//     * @param that   the `Future` whose result we want to use if this `Future` fails.
//     * @return       a `Future` with the successful result of this or that `Future` or the failure of this `Future` if both fail
//     * @group Transformations
//     */
//    fun <U, T : U> fallbackTo(that: Future<U>): Future<U> =
//        if (this == that) this
//        else {
//            val ec = internalExecutor
//            recoverWith(internalExecutor) { case _ => that } recoverWith { case _ => this }
//        }

//    /** Creates a new `Future[S]` which is completed with this `Future`'s result if
//     *  that conforms to `S`'s erased type or a `ClassCastException` otherwise.
//     *
//     * @tparam S     the type of the returned `Future`
//     * @param tag    the `ClassTag` which will be used to cast the result of this `Future`
//     * @return       a `Future` holding the casted result of this `Future` or a `ClassCastException` otherwise
//     * @group Transformations
//     */
//    fun <S> mapTo(tag: ClassTag<S>): Future<S> = {
//        implicit
//        val ec = internalExecutor
//        val boxedClass = {
//            val c = tag.runtimeClass
//            if (c.isPrimitive) Future.toBoxed(c) else c
//        }
//        require(boxedClass ne null)
//        map(s => boxedClass . cast (s).asInstanceOf[S])
//    }

    /** Applies the side-effecting function to the result of this future, and returns
     *  a new future with the result of this future.
     *
     *  This method allows one to enforce that the callbacks are executed in a
     *  specified order.
     *
     *  Note that if one of the chained `andThen` callbacks throws
     *  an exception, that exception is not propagated to the subsequent `andThen`
     *  callbacks. Instead, the subsequent `andThen` callbacks are given the original
     *  value of this future.
     *
     *  The following example prints out `5`:
     *
     *  {{{
     *  val f = Future { 5 }
     *  f andThen {
     *    case r => sys.error("runtime exception")
     *  } andThen {
     *    case Failure(t) => println(t)
     *    case Success(v) => println(v)
     *  }
     *  }}}
     *
     * $swallowsExceptions
     *
     * @tparam U     only used to accept any return type of the given `PartialFunction`
     * @param pf     a `PartialFunction` which will be conditionally applied to the outcome of this `Future`
     * @return       a `Future` which will be completed with the exact same outcome as this `Future` but after the `PartialFunction` has been executed.
     * @group Callbacks
     */
    fun <U> andThen(executor: ExecutionContext, pf: PartialFunction<Try<T>, U>): Future<T> =
        transform(executor) { result ->
            try {
                pf.invokeOrElse(result) { it }
            } catch (nonFatal: Exception) {
                executor.reportFailure(nonFatal)
            }

            result
        }
}
