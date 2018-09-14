package kollect.arrow.concurrent

/**
 * A context to be notified by `scala.concurrent.blocking` when
 * a thread is about to block. In effect this trait provides
 * the implementation for `scala.concurrent.Await`.
 * `scala.concurrent.Await.result()` and `scala.concurrent.Await.ready()`
 * locates an instance of `BlockContext` by first looking for one
 * provided through `BlockContext.withBlockContext()` and failing that,
 * checking whether `Thread.currentThread` is an instance of `BlockContext`.
 * So a thread pool can have its `java.lang.Thread` instances implement
 * `BlockContext`. There's a default `BlockContext` used if the thread
 * doesn't implement `BlockContext`.
 *
 * Typically, you'll want to chain to the previous `BlockContext`,
 * like this:
 * {{{
 *  val oldContext = BlockContext.current
 *  val myContext = new BlockContext {
 *    override def blockOn[T](thunk: =>T)(implicit permission: CanAwait): T = {
 *      // you'd have code here doing whatever you need to do
 *      // when the thread is about to block.
 *      // Then you'd chain to the previous context:
 *      oldContext.blockOn(thunk)
 *    }
 *  }
 *  BlockContext.withBlockContext(myContext) {
 *    // then this block runs with myContext as the handler
 *    // for scala.concurrent.blocking
 *  }
 *  }}}
 */
interface BlockContext {

    /** Used internally by the framework;
     * Designates (and eventually executes) a thunk which potentially blocks the calling `java.lang.Thread`.
     *
     * Clients must use `scala.concurrent.blocking` or `scala.concurrent.Await` instead.
     */
    fun <T> blockOn(permission: CanAwait, thunk: () -> T): T

    companion object {

        private object DefaultBlockContext : BlockContext {
            override fun <T> blockOn(permission: CanAwait, thunk: () -> T): T = thunk()
        }

        /**
         * @return the `BlockContext` that will be used if no other is found.
         **/
        fun defaultBlockContext(): BlockContext = DefaultBlockContext

        private val contextLocal = ThreadLocal<BlockContext>()

        /**
        @return the `BlockContext` that would be used for the current `java.lang.Thread` at this point
         **/
        fun current(): BlockContext {
            val ctx = contextLocal.get()
            return when (ctx) {
                null -> when (Thread.currentThread()) {
                    is BlockContext -> Thread.currentThread() as BlockContext
                    else -> DefaultBlockContext
                }
                else -> ctx
            }
        }

        /**
         * Installs a current `BlockContext` around executing `body`.
         **/
        fun <T> withBlockContext(blockContext: BlockContext, body: () -> T): T {
            val old = contextLocal.get() // can be null
            return try {
                contextLocal.set(blockContext)
                body()
            } finally {
                contextLocal.set(old)
            }
        }
    }
}
