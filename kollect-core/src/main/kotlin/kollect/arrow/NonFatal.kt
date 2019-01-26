package kollect.arrow

import kollect.ControlThrowable

/**
 * Extractor of non-fatal Throwables. Will not match fatal errors like `VirtualMachineError`
 * (for example, `OutOfMemoryError` and `StackOverflowError`, subclasses of `VirtualMachineError`), `ThreadDeath`,
 * `LinkageError`, `InterruptedException`, `ControlThrowable`.
 *
 * Note that [[scala.util.control.ControlThrowable]], an internal Throwable, is not matched by
 * `NonFatal` (and would therefore be thrown).
 *
 * For example, all harmless Throwables can be caught by:
 * {{{
 *   try {
 *     // dangerous stuff
 *   } catch {
 *     case NonFatal(e) => log.error(e, "Something not that bad.")
 *    // or
 *     case e if NonFatal(e) => log.error(e, "Something not that bad.")
 *   }
 * }}}
 */
object NonFatal {
    /**
     * Returns true if the provided `Throwable` is to be considered non-fatal, or false if it is to be considered fatal
     */
    inline operator fun invoke(t: Throwable): Boolean = when (t) {
        is VirtualMachineError, is ThreadDeath, is InterruptedException, is LinkageError, is ControlThrowable -> false
        // VirtualMachineError includes OutOfMemoryError and other fatal errors
        else -> true
    }
}
