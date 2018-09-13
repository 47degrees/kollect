package kollect

import arrow.Kind
import arrow.core.Either
import arrow.core.Tuple2
import arrow.core.getOrElse
import arrow.core.toOption
import arrow.data.NonEmptyList
import arrow.data.foldLeft
import arrow.effects.DeferredK
import arrow.effects.typeclasses.Async
import arrow.higherkind
import arrow.instance
import arrow.typeclasses.Applicative
import arrow.typeclasses.Monad
import arrow.typeclasses.binding

// Kollect queries
interface KollectRequest

// A query to a remote data source
sealed class KollectQuery<I : Any, A> : KollectRequest {
    abstract val dataSource: DataSource<I, A>
    abstract val identities: NonEmptyList<I>

    data class KollectOne<I : Any, A>(val id: I, val ds: DataSource<I, A>) : KollectQuery<I, A>() {
        override val identities: NonEmptyList<I> = NonEmptyList(id, emptyList())
        override val dataSource: DataSource<I, A> = ds
    }

    data class Batch<I : Any, A>(val ids: NonEmptyList<I>, val ds: DataSource<I, A>) : KollectQuery<I, A>() {
        override val identities: NonEmptyList<I> = ids
        override val dataSource: DataSource<I, A> = ds
    }
}

// Kollect result states
sealed class KollectStatus {
    data class KollectDone<A>(val result: A) : KollectStatus()
    object KollectMissing : KollectStatus()
}

// Kollect errors
sealed class KollectException : NoStackTrace() {
    abstract val environment: Env

    data class MissingIdentity(val i: Any, val request: KollectQuery<Any, Any>, override val environment: Env) : KollectException()
    data class UnhandledException(val e: Throwable, override val environment: Env) : KollectException()
}

// In-progress request
data class BlockedRequest<F>(val request: KollectRequest, val result: (KollectStatus) -> arrow.Kind<F, Unit>)

/* Combines the identities of two `KollectQuery` to the same data source. */
private fun <I : Any, A> combineIdentities(x: KollectQuery<I, A>, y: KollectQuery<I, A>): NonEmptyList<I> =
    y.identities.foldLeft(x.identities) { acc, i ->
        if (acc.contains(i)) acc else NonEmptyList(acc.head, acc.tail + i)
    }

/**
 * Combines two requests to the same data source.
 */
@Suppress("UNCHECKED_CAST")
private fun <I : Any, A, F> combineRequests(MF: Monad<F>, x: BlockedRequest<F>, y: BlockedRequest<F>): BlockedRequest<F> {
    val first = x.request
    val second = y.request
    return when {
        first is KollectQuery.KollectOne<*, *> && second is KollectQuery.KollectOne<*, *> -> {
            val first = (first as KollectQuery.KollectOne<I, A>)
            val second = (second as KollectQuery.KollectOne<I, A>)
            val aId = first.id
            val ds = first.ds
            val anotherId = second.id
            if (aId == anotherId) {
                val newRequest = KollectQuery.KollectOne(aId, ds)
                val newResult = { r: KollectStatus -> MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } } }
                BlockedRequest(newRequest, newResult)
            } else {
                val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
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
            val first = (first as KollectQuery.KollectOne<I, A>)
            val second = (second as KollectQuery.Batch<I, A>)
            val oneId = first.id
            val ds = first.ds

            val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
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
            val first = (first as KollectQuery.Batch<I, A>)
            val second = (second as KollectQuery.KollectOne<I, A>)
            val oneId = second.id
            val ds = first.ds

            val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
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
            val first = (first as KollectQuery.Batch<I, A>)
            val second = (second as KollectQuery.Batch<I, A>)
            val ds = first.ds

            val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
            val newResult = { r: KollectStatus -> MF.run { tupled(x.result(r), y.result(r)).flatMap { MF.just(Unit) } } }
            BlockedRequest(newRequest, newResult)
        }
    }
}

/* A map from data sources to blocked requests used to group requests to the same data source. */
data class RequestMap<F>(val m: Map<DataSource<Any, Any>, BlockedRequest<F>>)

/* Combine two `RequestMap` instances to batch requests to the same data source. */
private fun <I : Any, A, F> combineRequestMaps(MF: Monad<F>, x: RequestMap<F>, y: RequestMap<F>): RequestMap<F> =
    RequestMap(x.m.foldLeft(y.m) { acc, tuple ->
        val combinedReq: BlockedRequest<F> = acc[tuple.key].toOption().fold({ tuple.value }, { combineRequests<I, A, F>(MF, tuple.value, it) })
        acc.filterNot { it.key == tuple.key } + mapOf(tuple.key to combinedReq)
    })

// `Kollect` result data type
sealed class KollectResult<F, A> {
    data class Done<F, A>(val x: A) : KollectResult<F, A>()
    data class Blocked<F, A>(val rs: RequestMap<F>, val cont: Kollect<F, A>) : KollectResult<F, A>()
    data class Throw<F, A>(val e: (Env) -> KollectException) : KollectResult<F, A>()
}

// Kollect data type
@higherkind
sealed class Kollect<F, A> : KollectOf<F, A> {

    abstract val run: arrow.Kind<F, KollectResult<F, A>>

    data class Unkollect<F, A>(override val run: arrow.Kind<F, KollectResult<F, A>>) : Kollect<F, A>()

    companion object {
        /**
         * Lift a plain value to the Kollect monad.
         */
        fun <F, A> pure(AF: Applicative<F>, a: A): Kollect<F, A> = Unkollect(AF.just(KollectResult.Done(a)))

        fun <F, A> exception(AF: Applicative<F>, e: (Env) -> KollectException): Kollect<F, A> = Unkollect(AF.just(KollectResult.Throw(e)))

        fun <F, A> error(AF: Applicative<F>, e: Throwable): Kollect<F, A> = exception(AF) { env -> KollectException.UnhandledException(e, env) }

        @Suppress("UNCHECKED_CAST")
        inline operator fun <F, I : Any, A> invoke(AF: Async<F>, id: I, ds: DataSource<I, A>): Kollect<F, A> =
            Unkollect<F, A>(AF.binding {
                val deferred = DeferredK.just(KollectStatus.KollectDone(Unit))
                val request = KollectQuery.KollectOne(id, ds)
                val result = deferred

                val blocked = BlockedRequest<F>(request, result)
                val anyDs = ds as DataSource<Any, Any>
                val blockedRequest = RequestMap<F>(mapOf(anyDs to blocked))

                KollectResult.Blocked(blockedRequest, Unkollect<F, A>(
                    deferred.get().flatMap {
                        case FetchDone (a: A) =>
                        Applicative[F].pure(KollectResult.Done(a))
                        case FetchMissing () =>
                        Applicative[F].pure(KollectResult.Throw((env) => MissingIdentity (id, request, env)))
                    }
                ))
            })
    }
}

// Kollect ops
@instance(Kollect::class)
interface KollectMonad<F, Identity : Any, Result> : Monad<KollectPartialOf<F>> {

    fun MF(): Monad<F>

    override fun <A> just(a: A): Kollect<F, A> = Kollect.Unkollect(MF().just(KollectResult.Done(a)))

    override fun <A, B> Kind<KollectPartialOf<F>, A>.map(f: (A) -> B): Kollect<F, B> =
        Kollect.Unkollect(MF().binding {
            val kollect = this@map.fix().run.bind()
            val result = when (kollect) {
                is KollectResult.Done -> KollectResult.Done<F, B>(f(kollect.x))
                is KollectResult.Blocked -> KollectResult.Blocked(kollect.rs, kollect.cont.map(f))
                is KollectResult.Throw -> KollectResult.Throw<F, B>(kollect.e)
            }
            result
        })

    override fun <A, B> Kind<KollectPartialOf<F>, A>.product(fb: Kind<KollectPartialOf<F>, B>): Kollect<F, Tuple2<A, B>> =
        Kollect.Unkollect(MF().binding {
            val fab = MF().run { tupled(this@product.fix().run, fb.fix().run).bind() }
            val first = fab.a
            val second = fab.b
            val result = when {
                first is KollectResult.Throw -> KollectResult.Throw<F, Tuple2<A, B>>(first.e)
                first is KollectResult.Done && second is KollectResult.Done -> KollectResult.Done(Tuple2(first.x, second.x))
                first is KollectResult.Done && second is KollectResult.Blocked -> KollectResult.Blocked(second.rs, this@product.product(second.cont))
                first is KollectResult.Blocked && second is KollectResult.Done -> KollectResult.Blocked(first.rs, first.cont.product(fb))
                first is KollectResult.Blocked && second is KollectResult.Blocked -> KollectResult.Blocked(combineRequestMaps<Identity, Result, F>(MF(), first.rs, second.rs), first.cont.product(second.cont))
                // second is KollectResult.Throw
                else -> KollectResult.Throw((second as KollectResult.Throw).e)
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
                is KollectResult.Done -> f(kollect.x).fix()
                is KollectResult.Throw -> Kollect.Unkollect(MF().just(KollectResult.Throw(kollect.e)))
                // kollect is KollectResult.Blocked
                else -> Kollect.Unkollect(MF().just(KollectResult.Blocked((kollect as KollectResult.Blocked).rs, kollect.cont.flatMap(f))))
            }
            result.run.bind()
        })
}
