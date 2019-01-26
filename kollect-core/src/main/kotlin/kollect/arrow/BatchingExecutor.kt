package arrow.concurrent

import kollect.arrow.OnCompleteRunnable
import kollect.arrow.concurrent.BlockContext
import java.util.concurrent.Executor

/**
 * Executor which groups multiple nested `Runnable.run()` calls into a single Runnable passed to the original Executor.
 * This can be a useful optimization because it bypasses the original context's task queue and keeps related (nested)
 * code on a single thread which may improve CPU affinity. However, if tasks passed to the Executor are blocking or
 * expensive, this optimization can prevent work-stealing and make performance worse. Also, some ExecutionContext may be
 * fast enough natively that this optimization just adds overhead.
 *
 * The default ExecutionContext.global is already batching or fast enough not to benefit from it; while `fromExecutor`
 * and `fromExecutorService` do NOT add this optimization since they don't know whether the underlying executor will
 * benefit from it. A batching executor can create deadlocks if code does not use `scala.concurrent.blocking` when it
 * should, because tasks created within other tasks will block on the outer task completing.
 * This executor may run tasks in any order, including LIFO order.  There are no ordering guarantees.
 *
 * WARNING: The underlying Executor's execute-method must not execute the submitted Runnable in the calling thread
 * synchronously. It must enqueue/handoff the Runnable.
 */
abstract class BatchingExecutor : Executor {

    // invariant: if "tasksLocal.get ne null" then we are inside BatchingRunnable.run; if it is null, we are outside
    open val tasksLocal = ThreadLocal<List<Runnable>>()

    private inner class Batch(val initial: List<Runnable>) : Runnable, BlockContext {

        var parentBlockContext: BlockContext? = null

        // this method runs in the delegate ExecutionContext's thread
        override fun run(): Unit {
            require(tasksLocal.get() == null)

            val prevBlockContext = BlockContext.current()
            BlockContext.withBlockContext(this) {
                try {
                    parentBlockContext = prevBlockContext

                    fun processBatch(batch: List<Runnable>): Unit {
                        when {
                            batch.isEmpty() -> Unit
                            batch.isNotEmpty() -> {
                                tasksLocal.set(batch.drop(1))
                                try {
                                    batch.first().run()
                                } catch (e: Exception) {
                                    // if one task throws, move the remaining tasks to another thread so we can throw the
                                    // exception up to the invoking executor
                                    val remaining = tasksLocal.get()
                                    tasksLocal.set(listOf())
                                    unbatchedExecute(Batch(remaining)) //TODO what if this submission fails?
                                    throw e // rethrow
                                }
                            }
                        }
                        processBatch(tasksLocal.get()) // since head.run() can add entries, always do tasksLocal.get here
                    }

                    processBatch(initial)
                } finally {
                    tasksLocal.remove()
                    parentBlockContext = null
                }
            }
        }

        override fun <T> blockOn(permission: CanAwait, thunk: () -> T): T {
            // if we know there will be blocking, we don't want to keep tasks queued up because it could deadlock.
            val tasks = tasksLocal.get()
            tasksLocal.set(listOf())
            if ((tasks != null) && tasks.isNotEmpty()) {
                unbatchedExecute(Batch(tasks))
            }

            // now delegate the blocking to the previous BC
            require(parentBlockContext != null)
            return parentBlockContext!!.blockOn(permission, thunk)
        }
    }

    protected abstract fun unbatchedExecute(r: Runnable): Unit

    override fun execute(runnable: Runnable): Unit =
        if (batchable(runnable)) { // If we can batch the runnable
            val task = tasksLocal.get()
            when (task) {
                null -> unbatchedExecute(Batch(listOf(runnable))) // If we aren't in batching mode yet, enqueue batch
                else -> tasksLocal.set(listOf(runnable) + task) // If we are already in batching mode, add to batch
            }
        } else unbatchedExecute(runnable) // If not batchable, just delegate to underlying

    /** Override this to define which runnables will be batched. */
    private fun batchable(runnable: Runnable): Boolean = runnable is OnCompleteRunnable
}
