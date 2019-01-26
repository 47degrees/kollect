package kollect

import arrow.Kind
import arrow.data.NonEmptyList

sealed class KollectRequest

/**
 * Reified operations to query a data source. You can kollect one, or many (batch). `I` stands for the Identity used to
 * perform the query. `A` would be the result.
 */
sealed class KollectQuery<I, A> : KollectRequest() {
  abstract val dataSource: DataSource<I, A>
  abstract val identities: NonEmptyList<I>

  data class KollectOne<I, A>(val id: I, val ds: DataSource<I, A>) : KollectQuery<I, A>() {
    override val identities: NonEmptyList<I> = NonEmptyList(id, emptyList())
    override val dataSource: DataSource<I, A> = ds
  }

  data class Batch<I, A>(val ids: NonEmptyList<I>, val ds: DataSource<I, A>) : KollectQuery<I, A>() {
    override val identities: NonEmptyList<I> = ids
    override val dataSource: DataSource<I, A> = ds
  }
}

/**
 * Kollect query result states.
 */
sealed class KollectStatus {
  data class KollectDone<A>(val result: A) : KollectStatus()
  object KollectMissing : KollectStatus()
}

/**
 * Kollect query error types.
 */
sealed class KollectException {
  abstract val environment: Env

  data class MissingIdentity<I, A>(val i: I, val request: KollectQuery<I, A>, override val environment: Env) : KollectException()
  data class UnhandledException(val e: Throwable, override val environment: Env) : KollectException()

  fun toThrowable() = NoStackTrace(this)
}

/**
 * Ongoing request (currently in progress).
 */
data class BlockedRequest<F>(val request: KollectRequest, val result: (KollectStatus) -> Kind<F, Unit>)

/* A map from data sources to blocked requests used to group requests to the same data source. */
data class RequestMap<F>(val m: Map<DataSource<Any, Any>, BlockedRequest<F>>)

// `Kollect` result data type
sealed class KollectResult<F, A> {
  data class Done<F, A>(val x: A) : KollectResult<F, A>()
  data class Blocked<F, A>(val rs: RequestMap<F>, val cont: Kollect<F, A>) : KollectResult<F, A>()
  data class Throw<F, A>(val e: (Env) -> KollectException) : KollectResult<F, A>()
}
