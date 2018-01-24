package kollect

import arrow.data.NonEmptyList

//sealed class FetchError {
//    fun env(): Env
//}
//case class NotFound(env: Env, request: FetchOne[_, _]) extends FetchException
//case class MissingIdentities(env: Env, missing: Map[DataSourceName, List[Any]])
//extends FetchException
//case class UnhandledException(env: Env, err: Throwable) extends FetchException
//
//sealed class FetchRequest
//
//sealed class FetchQuery<I: Any, A> : FetchRequest() {
//    abstract fun dataSource(): DataSource<I, A>
//    abstract fun identities(): NonEmptyList<I>
//}
