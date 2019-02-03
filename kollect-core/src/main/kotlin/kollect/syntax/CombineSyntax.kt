@file:Suppress("FunctionName", "UNCHECKED_CAST")

package kollect

import arrow.core.getOrElse
import arrow.core.toOption
import arrow.data.NonEmptyList
import arrow.data.foldLeft
import arrow.typeclasses.Monad
import kollect.KollectQuery.Batch
import kollect.KollectQuery.KollectOne
import kollect.KollectStatus.KollectDone
import kollect.KollectStatus.KollectMissing

/**
 * Combines the identities of two [KollectQuery] to the same data source.
 */
private fun <I, A> combineIdentities(x: KollectQuery<I, A>, y: KollectQuery<I, A>): NonEmptyList<I> =
        y.identities.foldLeft(x.identities) { acc, i ->
            if (acc.contains(i)) acc else NonEmptyList(acc.head, acc.tail + i)
        }

/**
 * Combines two requests to the same data source.
 */
private fun <I, A, F> combineRequests(MF: Monad<F>, x: BlockedRequest<F>, y: BlockedRequest<F>): BlockedRequest<F> {
    val first = x.request
    val second = y.request
    return if (first is KollectOne<*, *> && second is KollectOne<*, *>) {
        val firstOp = (first as KollectOne<I, A>)
        val secondOp = (second as KollectOne<I, A>)
        val aId = firstOp.id
        val ds = firstOp.ds
        val anotherId = secondOp.id
        if (aId == anotherId) {
            val newRequest = KollectOne(aId, ds)
            val newResult = { r: KollectStatus -> MF.run { tupled(x.result(r), y.result(r)).unit() } }
            BlockedRequest(newRequest, newResult)
        } else {
            val newRequest = Batch(combineIdentities(firstOp, secondOp), ds)
            val newResult = { r: KollectStatus ->
                when (r) {
                    is KollectDone<*> -> {
                        r.result as Map<*, *>
                        val xResult = r.result[aId].toOption().map { KollectDone(it) }.getOrElse { KollectMissing }
                        val yResult = r.result[anotherId].toOption().map { KollectDone(it) }.getOrElse { KollectMissing }
                        MF.run { tupled(x.result(xResult), y.result(yResult)).unit() }
                    }
                    is KollectMissing ->
                        MF.run { tupled(x.result(r), y.result(r)).unit() }
                }
            }
            BlockedRequest(newRequest, newResult)
        }
    } else if (first is KollectOne<*, *> && second is Batch<*, *>) {
        val firstOp = (first as KollectOne<I, A>)
        val secondOp = (second as Batch<I, A>)
        val oneId = firstOp.id
        val ds = firstOp.ds

        val newRequest = Batch(combineIdentities(firstOp, secondOp), ds)
        val newResult = { r: KollectStatus ->
            when (r) {
                is KollectDone<*> -> {
                    r.result as Map<*, *>
                    val oneResult = r.result[oneId].toOption().map { KollectDone(it) }.getOrElse { KollectMissing }
                    MF.run { tupled(x.result(oneResult), y.result(r)).unit() }
                }
                is KollectMissing -> MF.run { tupled(x.result(r), y.result(r)).unit() }
            }
        }
        BlockedRequest(newRequest, newResult)
    } else if (first is Batch<*, *> && second is KollectOne<*, *>) {
        val firstOp = (first as Batch<I, A>)
        val secondOp = (second as KollectOne<I, A>)
        val oneId = secondOp.id
        val ds = firstOp.ds

        val newRequest = Batch(combineIdentities(firstOp, secondOp), ds)
        val newResult = { r: KollectStatus ->
            when (r) {
                is KollectDone<*> -> {
                    r.result as Map<*, *>
                    val oneResult = r.result[oneId].toOption().map { KollectDone(it) }.getOrElse { KollectMissing }
                    MF.run { tupled(x.result(r), y.result(oneResult)).unit() }
                }
                is KollectMissing -> MF.run { tupled(x.result(r), y.result(r)).unit() }
            }
        }
        BlockedRequest(newRequest, newResult)
    }
    // first is Batch<*, *> && second is Batch<*, *>
    else {
        val firstOp = (first as Batch<I, A>)
        val secondOp = (second as Batch<I, A>)
        val ds = firstOp.ds

        val newRequest = Batch(combineIdentities(firstOp, secondOp), ds)
        val newResult = { r: KollectStatus -> MF.run { tupled(x.result(r), y.result(r)).unit() } }
        BlockedRequest(newRequest, newResult)
    }
}

/**
 * Combine two [RequestMap] instances to batch requests to the same data source.
 */
internal fun <I, A, F> combineRequestMaps(MF: Monad<F>, x: RequestMap<F>, y: RequestMap<F>): RequestMap<F> =
        RequestMap(x.m.foldLeft(y.m) { acc, tuple ->
            val combinedReq: BlockedRequest<F> = acc[tuple.key].toOption().fold({ tuple.value }, { combineRequests<I, A, F>(MF, tuple.value, it) })
            acc.updated(tuple.key, combinedReq)
        })
