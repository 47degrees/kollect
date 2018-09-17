package kollect

open class NoStackTrace(e: KollectException?) : Throwable(message = e.toString()) {
    override fun fillInStackTrace(): Throwable = this
}

open class ControlThrowable : NoStackTrace(null)

class NonLocalReturnControl(val key: Any, val value: Any) : ControlThrowable() {
    override fun fillInStackTrace(): Throwable = this
}
