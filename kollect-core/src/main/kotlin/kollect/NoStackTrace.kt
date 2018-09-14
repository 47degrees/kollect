package kollect

class NoStackTrace : Throwable() {
    override fun fillInStackTrace(): Throwable = this
}
