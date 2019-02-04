package kollect

open class NoStackTrace(e: KollectException) : Throwable(message = e.javaClass.simpleName)
