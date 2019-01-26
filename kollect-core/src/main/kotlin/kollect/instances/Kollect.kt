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
import kollect.Kollect.Unkollect
import kollect.KollectQuery.Batch
import kollect.KollectQuery.KollectOne
import kollect.KollectResult.Blocked
import kollect.KollectResult.Done
import kollect.KollectResult.Throw
import kollect.KollectStatus.KollectDone
import kollect.KollectStatus.KollectMissing

// Kollect ops
@instance(Kollect::class)
interface KollectMonad<F, I, A> : Monad<KollectPartialOf<F>> {

    fun MF(): Monad<F>

    override fun <A> just(a: A): Kollect<F, A> = Unkollect(MF().just(Done(a)))

    override fun <A, B> Kind<KollectPartialOf<F>, A>.map(f: (A) -> B): Kollect<F, B> =
        Unkollect(MF().binding {
            val kollect = this@map.fix().run.bind()
            val result = when (kollect) {
                is Done -> Done<F, B>(f(kollect.x))
                is Blocked -> Blocked(kollect.rs, kollect.cont.map(f))
                is Throw -> Throw(kollect.e)
            }
            result
        })

    override fun <A, B> Kind<KollectPartialOf<F>, A>.product(fb: Kind<KollectPartialOf<F>, B>): Kollect<F, Tuple2<A, B>> =
        Unkollect(MF().binding {
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

    override fun <A, B> Kind<KollectPartialOf<F>, A>.flatMap(f: (A) -> Kind<KollectPartialOf<F>, B>): Kollect<F, B> = MF().run {
        Unkollect(this@flatMap.fix().run.flatMap {
            when (it) {
                is Done -> f(it.x).fix().run
                is Throw -> MF().just(Throw(it.e))
                is Blocked -> MF().just(Blocked(it.rs, it.cont.flatMap(f)))
            }
        })
    }
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
        first is KollectOne<*, *> && second is KollectOne<*, *> -> {
            val firstOp = (first as KollectOne<I, A>)
            val secondOp = (second as KollectOne<I, A>)
            val aId = firstOp.id
            val ds = firstOp.ds
            val anotherId = secondOp.id
            if (aId == anotherId) {
                val newRequest = KollectOne(aId, ds)
                val newResult = { r: KollectStatus -> MF.run { tupled(x.result(r), y.result(r)).void() } }
                BlockedRequest(newRequest, newResult)
            } else {
                val newRequest = Batch(combineIdentities(firstOp, secondOp), ds)
                val newResult = { r: KollectStatus ->
                    when (r) {
                        is KollectDone<*> -> {
                            r.result as Map<*, *>
                            val xResult = r.result[aId].toOption().map { KollectDone(it) }.getOrElse { KollectMissing }
                            val yResult = r.result[anotherId].toOption().map { KollectDone(it) }.getOrElse { KollectMissing }
                            MF.run { tupled(x.result(xResult), y.result(yResult)).void() }
                        }

                        is KollectMissing ->
                            MF.run { tupled(x.result(r), y.result(r)).void() }
                    }
                }
                BlockedRequest(newRequest, newResult)
            }
        }
        first is KollectOne<*, *> && second is Batch<*, *> -> {
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
                        MF.run { tupled(x.result(oneResult), y.result(r)).void() }
                    }
                    is KollectMissing -> MF.run { tupled(x.result(r), y.result(r)).void() }
                }
            }
            BlockedRequest(newRequest, newResult)
        }
        first is Batch<*, *> && second is KollectOne<*, *> -> {
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
                        MF.run { tupled(x.result(r), y.result(oneResult)).void() }
                    }
                    is KollectMissing -> MF.run { tupled(x.result(r), y.result(r)).void() }
                }
            }
            BlockedRequest(newRequest, newResult)
        }
        // first is Batch<*, *> && second is Batch<*, *>
        else -> {
            val firstOp = (first as Batch<I, A>)
            val secondOp = (second as Batch<I, A>)
            val ds = firstOp.ds

            val newRequest = Batch(combineIdentities(firstOp, secondOp), ds)
            val newResult = { r: KollectStatus -> MF.run { tupled(x.result(r), y.result(r)).void() } }
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
