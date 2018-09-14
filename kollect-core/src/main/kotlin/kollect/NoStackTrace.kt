package kollect

open class NoStackTrace : Throwable() {
    override fun fillInStackTrace(): Throwable = this
}

open class ControlThrowable : NoStackTrace()

class NonLocalReturnControl(val key: Any, val value: Any) : ControlThrowable() {
    override fun fillInStackTrace(): Throwable = this
}