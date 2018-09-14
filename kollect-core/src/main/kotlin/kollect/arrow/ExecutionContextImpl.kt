package kollect.arrow

import arrow.concurrent.CanAwait
import arrow.core.Option
import arrow.core.getOrElse
import arrow.core.toOption
import kollect.arrow.concurrent.BlockContext
import java.util.concurrent.Callable
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinWorkerThread
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

abstract class ExecutionContextPlusExecutionService(executor: Executor, reporter: (Throwable) -> Unit) : ExecutionContextImpl(executor, reporter), ExecutionContextExecutorService

open class ExecutionContextImpl(val executor: Executor, val reporter: (Throwable) -> Unit) : ExecutionContextExecutor {
    override fun execute(runnable: Runnable) = executor.execute(runnable)
    override fun reportFailure(t: Throwable) = reporter(t)

    companion object {

        // Implement BlockContext on FJP threads
        open class DefaultThreadFactory(
            val daemonic: Boolean,
            val maxThreads: Int,
            val prefix: String,
            val uncaught: Thread.UncaughtExceptionHandler) : ThreadFactory, ForkJoinPool.ForkJoinWorkerThreadFactory {

            private val currentNumberOfThreads = AtomicInteger(0)

            private fun reserveThread(): Boolean {
                val threadsNow = currentNumberOfThreads.get()
                return when (threadsNow) {
                    maxThreads, Int.MAX_VALUE -> false
                    else -> currentNumberOfThreads.compareAndSet(threadsNow, threadsNow + 1) || reserveThread()
                }
            }

            private fun deregisterThread(): Boolean {
                val threadsNow = currentNumberOfThreads.get()
                return when (threadsNow) {
                    0 -> false
                    else -> currentNumberOfThreads.compareAndSet(threadsNow, threadsNow - 1) || deregisterThread()
                }
            }

            fun <T : Thread> wire(thread: T): T {
                thread.isDaemon = daemonic
                thread.uncaughtExceptionHandler = uncaught
                thread.name = prefix + "-" + thread.getId()
                return thread
            }

            // As per ThreadFactory contract newThread should return `null` if cannot create new thread.
            override fun newThread(runnable: Runnable): Thread? = if (reserveThread()) {
                wire(Thread(Runnable {
                    // We have to decrement the current thread count when the thread exits
                    try {
                        runnable.run()
                    } finally {
                        deregisterThread()
                    }
                }))
            } else null

            override fun newThread(fjp: ForkJoinPool): ForkJoinWorkerThread? = if (reserveThread()) {
                wire(object : ForkJoinWorkerThread(fjp), BlockContext {
                    override fun onTermination(exception: Throwable?): Unit {
                        deregisterThread()
                    }

                    override fun <T> blockOn(permission: CanAwait, thunk: () -> T): T {
                        var result: T = null as T
                        ForkJoinPool.managedBlock(object : ForkJoinPool.ManagedBlocker {
                            var isdone = false
                            override fun block(): Boolean {
                                result = try {
                                    // When we block, switch out the BlockContext temporarily so that nested blocking does not created N new Threads
                                    BlockContext.withBlockContext(BlockContext.defaultBlockContext()) { thunk() }
                                } finally {
                                    isdone = true
                                }
                                return true
                            }

                            override fun isReleasable() = isdone
                        })
                        return result
                    }
                })
            } else null
        }

        fun createDefaultExecutorService(reporter: (Throwable) -> Unit): ExecutorService {
            fun getInt(name: String, default: String): Int {
                val a = try {
                    System.getProperty(name, default)
                } catch (e: Exception) {
                    if (e is SecurityException) {
                        default
                    } else default
                }
                return when {
                    a[0] == 'x' -> Math.ceil(Runtime.getRuntime().availableProcessors() * a.toString().substring(1).toDouble()).toInt()
                    else -> a.toInt()
                }
            }

            fun range(floor: Int, desired: Int, ceiling: Int) = Math.min(Math.max(floor, desired), ceiling)

            val numThreads = getInt("scala.concurrent.context.numThreads", "x1")

            // The hard limit on the number of active threads that the thread factory will produce
            // scala/bug#8955 Deadlocks can happen if maxNoOfThreads is too low, although we're currently not sure
            //         about what the exact threshold is. numThreads + 256 is conservatively high.
            val maxNoOfThreads = getInt("scala.concurrent.context.maxThreads", "x1")

            val desiredParallelism = range(
                getInt("scala.concurrent.context.minThreads", "1"),
                numThreads,
                maxNoOfThreads)

            // The thread factory must provide additional threads to support managed blocking.
            val maxExtraThreads = getInt("scala.concurrent.context.maxExtraThreads", "256")

            val uncaughtExceptionHandler: Thread.UncaughtExceptionHandler = object : Thread.UncaughtExceptionHandler {
                override fun uncaughtException(thread: Thread, cause: Throwable): Unit = reporter(cause)
            }

            val threadFactory = DefaultThreadFactory(daemonic = true,
                maxThreads = maxNoOfThreads + maxExtraThreads,
                prefix = "scala-execution-context-global",
                uncaught = uncaughtExceptionHandler)

            return ForkJoinPool(desiredParallelism, threadFactory, uncaughtExceptionHandler, true)
        }

        fun fromExecutor(e: Executor?, reporter: (Throwable) -> Unit = ExecutionContext.defaultReporter()): ExecutionContextImpl =
            ExecutionContextImpl(e.toOption().getOrElse { createDefaultExecutorService(reporter) }, reporter)

        fun fromExecutorService(es: ExecutorService, reporter: (Throwable) -> Unit = ExecutionContext.defaultReporter())
            : ExecutionContextPlusExecutionService =
            object : ExecutionContextPlusExecutionService(Option(es).getOrElse { createDefaultExecutorService(reporter) }, reporter) {

                fun asExecutorService(): ExecutorService = executor as ExecutorService

                override fun execute(command: Runnable) = executor.execute(command)

                override fun shutdown() {
                    asExecutorService().shutdown()
                }

                override fun shutdownNow() = asExecutorService().shutdownNow()

                override fun isShutdown() = asExecutorService().isShutdown

                override fun isTerminated() = asExecutorService().isTerminated

                override fun awaitTermination(l: Long, timeUnit: TimeUnit) = asExecutorService().awaitTermination(l, timeUnit)

                override fun <T : Any?> submit(callable: Callable<T>) = asExecutorService().submit(callable)

                override fun <T : Any?> submit(runnable: Runnable, t: T) = asExecutorService().submit(runnable, t)

                override fun submit(runnable: Runnable) = asExecutorService().submit(runnable)

                override fun <T : Any?> invokeAll(callables: MutableCollection<out Callable<T>>?): MutableList<Future<T>> =
                    asExecutorService().invokeAll(callables)

                override fun <T : Any?> invokeAll(callables: MutableCollection<out Callable<T>>?, timeout: Long, unit: TimeUnit?): MutableList<Future<T>> =
                    asExecutorService().invokeAll(callables, timeout, unit)

                override fun <T : Any?> invokeAny(callables: MutableCollection<out Callable<T>>?): T = asExecutorService().invokeAny(callables)

                override fun <T : Any?> invokeAny(callables: MutableCollection<out Callable<T>>?, timeout: Long, unit: TimeUnit?): T =
                    asExecutorService().invokeAny(callables, timeout, unit)
            }
    }
}
