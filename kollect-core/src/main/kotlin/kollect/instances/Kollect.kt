@file:Suppress("FunctionName", "UNCHECKED_CAST")

package kollect

import arrow.Kind
import arrow.core.Either
import arrow.core.Tuple2
import arrow.core.getOrElse
import arrow.core.toOption
import arrow.data.NonEmptyList
import arrow.data.foldLeft
import arrow.instance
import arrow.typeclasses.Monad
import arrow.typeclasses.binding
import kollect.KollectResult.Blocked
import kollect.KollectResult.Done
import kollect.KollectResult.Throw

// Kollect ops
@instance(Kollect::class)
interface KollectMonad<F, I, A> : Monad<KollectPartialOf<F>> {

    fun MF(): Monad<F>

    override fun <A> just(a: A): Kollect<F, A> = Kollect.Unkollect(MF().just(Done(a)))

    override fun <A, B> Kind<KollectPartialOf<F>, A>.map(f: (A) -> B): Kollect<F, B> =
        Kollect.Unkollect(MF().binding {
            val kollect = this@map.fix().run.bind()
            val result = when (kollect) {
                is Done -> Done<F, B>(f(kollect.x))
                is Blocked -> Blocked(kollect.rs, kollect.cont.map(f))
                is Throw -> Throw(kollect.e)
            }
            result
        })

    override fun <A, B> Kind<KollectPartialOf<F>, A>.product(fb: Kind<KollectPartialOf<F>, B>): Kollect<F, Tuple2<A, B>> =
        Kollect.Unkollect(MF().binding {
            val fab = MF().run { tupled(this@product.fix().run, fb.fix().run).bind() }
            val first = fab.a
            val second = fab.b
            val result = when {
                first is Throw -> Throw<F, Tuple2<A, B>>(first.e)
                first is Done && second is Done -> Done(Tuple2(first.x, second.x))
                first is Done && second is Blocked -> Blocked(second.rs, this@product.product(second.cont))
                first is Blocked && second is Done -> Blocked(first.rs, first.cont.product(fb))
                first is Blocked && second is Blocked -> Blocked(combineRequestMaps<I, A, F>(MF(), first.rs, second.rs), first.cont.product(second.cont))
                // second is Throw
                else -> Throw((second as Throw).e)
            }
            result
        })

    override fun <A, B> tailRecM(a: A, f: (A) -> Kind<KollectPartialOf<F>, Either<A, B>>): Kollect<F, B> =
        f(a).flatMap {
            when (it) {
                is Either.Left -> tailRecM(a, f)
                is Either.Right -> just(it.b)
            }
        }.fix()

    override fun <A, B> Kind<KollectPartialOf<F>, A>.flatMap(f: (A) -> Kind<KollectPartialOf<F>, B>): Kollect<F, B> =
        Kollect.Unkollect(MF().binding {
            val kollect = this@flatMap.fix().run.bind()
            val result: Kollect<F, B> = when (kollect) {
                is Done -> f(kollect.x).fix()
                is Throw -> Kollect.Unkollect(MF().just(Throw(kollect.e)))
                // kollect is Blocked
                else -> Kollect.Unkollect(MF().just(Blocked((kollect as Blocked).rs, kollect.cont.flatMap(f))))
            }
            result.run.bind()
        })
}

/* Combine two `RequestMap` instances to batch requests to the same data source. */
private fun <I, A, F> combineRequestMaps(MF: Monad<F>, x: RequestMap<F>, y: RequestMap<F>): RequestMap<F> =
    RequestMap(x.m.foldLeft(y.m) { acc, tuple ->
        val combinedReq: BlockedRequest<F> = acc[tuple.key].toOption().fold({ tuple.value }, { combineRequests<I, A, F>(MF, tuple.value, it) })
        acc.filterNot { it.key == tuple.key } + mapOf(tuple.key to combinedReq)
    })


/**
 * Combines two requests to the same data source.
 */
private fun <I, A, F> combineRequests(MF: Monad<F>, x: BlockedRequest<F>, y: BlockedRequest<F>): BlockedRequest<F> {
    val first = x.request
    val second = y.request
    return when {
        first is KollectQuery.KollectOne<*, *> && second is KollectQuery.KollectOne<*, *> -> {
            val firstOp = (first as KollectQuery.KollectOne<I, A>)
            val secondOp = (second as KollectQuery.KollectOne<I, A>)
            val aId = firstOp.id
            val ds = firstOp.ds
            val anotherId = secondOp.id
            if (aId == anotherId) {
                val newRequest = KollectQuery.KollectOne(aId, ds)
                val newResult = { r: KollectStatus -> MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } } }
                BlockedRequest(newRequest, newResult)
            } else {
                val newRequest = KollectQuery.Batch(combineIdentities(firstOp, secondOp), ds)
                val newResult = { r: KollectStatus ->
                    when (r) {
                        is KollectStatus.KollectDone<*> -> {
                            r.result as Map<*, *>
                            val xResult = r.result[aId].toOption().map { KollectStatus.KollectDone(it) }.getOrElse { KollectStatus.KollectMissing }
                            val yResult = r.result[anotherId].toOption().map { KollectStatus.KollectDone(it) }.getOrElse { KollectStatus.KollectMissing }
                            MF.run { tupled(x.result(xResult), y.result(yResult)).flatMap { MF.just(Unit) } }
                        }

                        is KollectStatus.KollectMissing ->
                            MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } }
                    }
                }
                BlockedRequest(newRequest, newResult)
            }
        }
        first is KollectQuery.KollectOne<*, *> && second is KollectQuery.Batch<*, *> -> {
            val firstOp = (first as KollectQuery.KollectOne<I, A>)
            val secondOp = (second as KollectQuery.Batch<I, A>)
            val oneId = firstOp.id
            val ds = firstOp.ds

            val newRequest = KollectQuery.Batch(combineIdentities(firstOp, secondOp), ds)
            val newResult = { r: KollectStatus ->
                when (r) {
                    is KollectStatus.KollectDone<*> -> {
                        r.result as Map<*, *>
                        val oneResult = r.result[oneId].toOption().map { KollectStatus.KollectDone(it) }.getOrElse { KollectStatus.KollectMissing }
                        MF.run { tupled(x.result(oneResult), y.result(r)).flatMap { MF.just(Unit) } }
                    }
                    is KollectStatus.KollectMissing -> MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } }
                }
            }
            BlockedRequest(newRequest, newResult)
        }
        first is KollectQuery.Batch<*, *> && second is KollectQuery.KollectOne<*, *> -> {
            val firstOp = (first as KollectQuery.Batch<I, A>)
            val secondOp = (second as KollectQuery.KollectOne<I, A>)
            val oneId = secondOp.id
            val ds = firstOp.ds

            val newRequest = KollectQuery.Batch(combineIdentities(firstOp, secondOp), ds)
            val newResult = { r: KollectStatus ->
                when (r) {
                    is KollectStatus.KollectDone<*> -> {
                        r.result as Map<*, *>
                        val oneResult = r.result[oneId].toOption().map { KollectStatus.KollectDone(it) }.getOrElse { KollectStatus.KollectMissing }
                        MF.run { tupled(x.result(r), y.result(oneResult)).flatMap { MF.just(Unit) } }
                    }
                    is KollectStatus.KollectMissing -> MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } }
                }
            }
            BlockedRequest(newRequest, newResult)
        }
        // first is KollectQuery.Batch<*, *> && second is KollectQuery.Batch<*, *>
        else -> {
            val firstOp = (first as KollectQuery.Batch<I, A>)
            val secondOp = (second as KollectQuery.Batch<I, A>)
            val ds = firstOp.ds

            val newRequest = KollectQuery.Batch(combineIdentities(firstOp, secondOp), ds)
            val newResult = { r: KollectStatus -> MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } } }
            BlockedRequest(newRequest, newResult)
        }
    }
}

/**
 * Combines the identities of two `KollectQuery` to the same data source.
 */
private fun <I, A> combineIdentities(x: KollectQuery<I, A>, y: KollectQuery<I, A>): NonEmptyList<I> =
    y.identities.foldLeft(x.identities) { acc, i ->
        if (acc.contains(i)) acc else NonEmptyList(acc.head, acc.tail + i)
    }
