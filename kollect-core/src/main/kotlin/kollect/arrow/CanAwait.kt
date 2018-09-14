package arrow.concurrent

sealed class CanAwait
object AwaitPermission : CanAwait()