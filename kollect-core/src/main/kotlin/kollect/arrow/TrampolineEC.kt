package kollect.arrow

import arrow.concurrent.CanAwait
import arrow.effects.Logger
import kollect.arrow.concurrent.BlockContext

/**
 * INTERNAL API — a [[scala.concurrent.ExecutionContext]] implementation
 * that executes runnables immediately, on the current thread,
 * by means of a trampoline implementation.
 *
 * Can be used in some cases to keep the asynchronous execution
 * on the current thread, as an optimization, but be warned,
 * you have to know what you're doing.
 *
 * This is the JVM-specific implementation, for which we
 * need a `ThreadLocal` and Scala's `BlockingContext`.
 */
open class TrampolineEC private constructor(val underlying: ExecutionContext) : ExecutionContext {

    private val trampoline = object : ThreadLocal<Trampoline>() {
        override fun initialValue(): Trampoline = JVMTrampoline(underlying)
    }

    override fun execute(runnable: Runnable): Unit {
        trampoline.get().execute(runnable)
    }

    override fun reportFailure(cause: Throwable): Unit {
        underlying.reportFailure(cause)
    }

    companion object {

        /**
         * Overrides [[Trampoline]] to be `BlockContext`-aware.
         *
         * INTERNAL API.
         */
        class JVMTrampoline(underlying: ExecutionContext) : Trampoline(underlying) {

            private val trampolineContext: BlockContext = object : BlockContext {
                override fun <T> blockOn(permission: CanAwait, thunk: () -> T): T {
                    // In case of blocking, execute all scheduled local tasks on
                    // a separate thread, otherwise we could end up with a dead-lock
                    forkTheRest()
                    return thunk()
                }
            }

            override fun startLoop(runnable: Runnable): Unit {
                BlockContext.withBlockContext(trampolineContext) {
                    super.startLoop(runnable)
                }
            }
        }

        /**
         * [[TrampolineEC]] instance that executes everything
         * immediately, on the current thread.
         *
         * Implementation notes:
         *
         *  - if too many `blocking` operations are chained, at some point
         *    the implementation will trigger a stack overflow error
         *  - `reportError` re-throws the exception in the hope that it
         *    will get caught and reported by the underlying thread-pool,
         *    because there's nowhere it could report that error safely
         *    (i.e. `System.err` might be routed to `/dev/null` and we'd
         *    have no way to override it)
         *
         * INTERNAL API.
         */
        val immediate: TrampolineEC = object : TrampolineEC(object : ExecutionContext {
            override fun execute(runnable: Runnable) = runnable.run()

            override fun reportFailure(cause: Throwable): Unit {
                Logger.reportFailure(cause)
            }
        }) {}
    }
}
