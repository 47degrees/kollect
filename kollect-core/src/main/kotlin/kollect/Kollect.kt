package kollect

import arrow.core.getOrElse
import arrow.core.toOption
import arrow.data.NonEmptyList
import arrow.data.foldLeft
import arrow.effects.IO
import arrow.effects.applicative
import arrow.effects.fix

// Fetch queries
interface KollectRequest

// A query to a remote data source
sealed class KollectQuery<I : Any, A> : KollectRequest {
    abstract val dataSource: DataSource<I, A>
    abstract val identities: NonEmptyList<I>

    data class FetchOne<I : Any, A>(val id: I, val ds: DataSource<I, A>) : KollectQuery<I, A>() {
        override val identities: NonEmptyList<I> = NonEmptyList(id, emptyList())
        override val dataSource: DataSource<I, A> = ds
    }

    data class Batch<I : Any, A>(val ids: NonEmptyList<I>, val ds: DataSource<I, A>) : KollectQuery<I, A>() {
        override val identities: NonEmptyList<I> = ids
        override val dataSource: DataSource<I, A> = ds
    }
}

// Fetch result states
sealed class KollectStatus {
    data class FetchDone<A>(val result: A) : KollectStatus()
    object FetchMissing : KollectStatus()
}

// Fetch errors
sealed class FetchException : NoStackTrace() {
    abstract val environment: Env

    data class MissingIdentity(val i: Any, val request: KollectQuery<Any, Any>, override val environment: Env) : FetchException()
    data class UnhandledException(val e: Throwable, override val environment: Env) : FetchException()
}

// In-progress request
data class BlockedRequest(val request: KollectRequest, val result: (KollectStatus) -> IO<Unit>)

/* Combines the identities of two `FetchQuery` to the same data source. */
private fun <I : Any, A> combineIdentities(x: KollectQuery<I, A>, y: KollectQuery<I, A>): NonEmptyList<I> =
    y.identities.foldLeft(x.identities) { acc, i ->
        if (acc.contains(i)) acc else NonEmptyList(acc.head, acc.tail + i)
    }

/* Combines two requests to the same data source. */
private fun <I : Any, A> combineRequests(x: BlockedRequest, y: BlockedRequest): BlockedRequest {
    val requests = Pair(x.request, y.request)
    return when (requests) {
        is (KollectQuery.FetchOne<I, A>, KollectQuery.FetchOne<I, A>) -> {
            val first = (requests.first as KollectQuery.FetchOne<I, A>)
            val second = (requests.second as KollectQuery.FetchOne<I, A>)
            val aId = first.id
            val ds = first.ds
            val anotherId = second.id
            if (aId == anotherId) {
                val newRequest = KollectQuery.FetchOne(aId, ds)
                val newResult = { r: KollectStatus -> IO.applicative().tupled(x.result(r), y.result(r)).fix().flatMap { IO.unit } }
                BlockedRequest(newRequest, newResult)
            } else {
                val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
                val newResult = { r: KollectStatus ->
                    when (r) {
                        is KollectStatus.FetchDone<*> -> {
                            r.result as Map<*, *>
                            val xResult = r.result[aId].toOption().map { KollectStatus.FetchDone(it) }.getOrElse { KollectStatus.FetchMissing }
                            val yResult = r.result[anotherId].toOption().map { KollectStatus.FetchDone(it) }.getOrElse { KollectStatus.FetchMissing }
                            IO.applicative().tupled(x.result(xResult), y.result(yResult)).fix().flatMap { IO.unit }
                        }

                        is KollectStatus.FetchMissing ->
                            IO.applicative().tupled(x.result(r), y.result(r)).fix().flatMap { IO.unit }
                    }
                }
                BlockedRequest(newRequest, newResult)
            }
        }
        is (KollectQuery.FetchOne<I, A>, KollectQuery.Batch<I, A>) -> {
            val first = (requests.first as KollectQuery.FetchOne<I, A>)
            val second = (requests.second as KollectQuery.Batch<I, A>)
            val oneId = first.id
            val ds = first.ds

            val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
            val newResult = { r: KollectStatus ->
                when (r) {
                    is KollectStatus.FetchDone<*> -> {
                        r.result as Map<*, *>
                        val oneResult = r.result[oneId].toOption().map { KollectStatus.FetchDone(it) }.getOrElse { KollectStatus.FetchMissing }
                        IO.applicative().tupled(x.result(oneResult), y.result(r)).fix().flatMap { IO.unit }
                    }
                    is KollectStatus.FetchMissing -> IO.applicative().tupled(x.result(r), y.result(r)).fix().flatMap { IO.unit }
                }
            }
            BlockedRequest(newRequest, newResult)
        }
        is (KollectQuery.Batch<I, A>, KollectQuery.FetchOne<I, A>) -> {
            val first = (requests.first as KollectQuery.Batch<I, A>)
            val second = (requests.second as KollectQuery.FetchOne<I, A>)
            val oneId = second.id
            val ds = first.ds

            val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
            val newResult = { r: KollectStatus ->
                when (r) {
                    is KollectStatus.FetchDone<*> -> {
                        r.result as Map<*, *>
                        val oneResult = r.result[oneId].toOption().map { KollectStatus.FetchDone(it) }.getOrElse { KollectStatus.FetchMissing }
                        IO.applicative().tupled(x.result(r), y.result(oneResult)).fix().flatMap { IO.unit }
                    }
                    is KollectStatus.FetchMissing -> IO.applicative().tupled(x.result(r), y.result(r)).fix().flatMap { IO.unit }
                }
            }
            BlockedRequest(newRequest, newResult)
        }
        // is (KollectQuery.Batch<I, A>, KollectQuery.Batch<I, A>)
        else -> {
            val first = (requests.first as KollectQuery.Batch<I, A>)
            val second = (requests.second as KollectQuery.Batch<I, A>)
            val ds = first.ds

            val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
            val newResult = { r: KollectStatus -> IO.applicative().tupled(x.result(r), y.result(r)).fix().flatMap { IO.unit } }
            BlockedRequest(newRequest, newResult)
        }
    }
}

/* A map from data sources to blocked requests used to group requests to the same data source. */
data class RequestMap(val m: Map<DataSource<Any, Any>, BlockedRequest>)

/* Combine two `RequestMap` instances to batch requests to the same data source. */
private fun <I : Any, A> combineRequestMaps(x: RequestMap, y: RequestMap): RequestMap =
    RequestMap(x.m.foldLeft(y.m) { acc, tuple ->
        val combinedReq: BlockedRequest = acc.get(tuple.key).toOption().fold({ tuple.value }, { combineRequests<I, A>(tuple.value, it) })
        acc.filterNot { it.key == tuple.key } + mapOf(tuple.key to combinedReq)
    })
