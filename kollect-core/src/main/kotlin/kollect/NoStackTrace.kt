package kollect

abstract class NoStackTrace : Throwable() {
    override fun fillInStackTrace(): Throwable = this
}
