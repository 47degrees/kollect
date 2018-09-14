package arrow.effects

/**
 * INTERNAL API — logs uncaught exceptions in a platform specific way.
 *
 * For the JVM logging is accomplished using the current
 * [[https://docs.oracle.com/javase/8/docs/api/java/lang/Thread.UncaughtExceptionHandler.html Thread.UncaughtExceptionHandler]].
 *
 * If an `UncaughtExceptionHandler` is not currently set,
 * then error is printed on standard output.
 */
object Logger {
    /** Logs an uncaught error. */
    fun reportFailure(e: Throwable): Unit = Thread.getDefaultUncaughtExceptionHandler().let {
        when (it) {
            null -> e.printStackTrace()
            else -> it.uncaughtException(Thread.currentThread(), e)
        }
    }
}
