package kollect

import arrow.Kind
import arrow.core.Either
import arrow.core.Tuple2
import arrow.core.getOrElse
import arrow.core.toOption
import arrow.data.NonEmptyList
import arrow.data.foldLeft
import arrow.effects.DeferredK
import arrow.effects.ForIO
import arrow.effects.IO
import arrow.effects.applicative
import arrow.effects.fix
import arrow.effects.monad
import arrow.higherkind
import arrow.instance
import arrow.typeclasses.Monad
import arrow.typeclasses.binding
import kollect.arrow.ContextShift
import kotlinx.coroutines.experimental.Deferred

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
data class BlockedRequest(val request: KollectRequest, val result: (KollectStatus) -> IO<Unit>)

/* Combines the identities of two `KollectQuery` to the same data source. */
private fun <I : Any, A> combineIdentities(x: KollectQuery<I, A>, y: KollectQuery<I, A>): NonEmptyList<I> =
    y.identities.foldLeft(x.identities) { acc, i ->
        if (acc.contains(i)) acc else NonEmptyList(acc.head, acc.tail + i)
    }

/* Combines two requests to the same data source. */
private fun <I : Any, A> combineRequests(x: BlockedRequest, y: BlockedRequest): BlockedRequest {
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
                val newResult = { r: KollectStatus -> IO.applicative().tupled(x.result(r), y.result(r)).fix().flatMap { IO.unit } }
                BlockedRequest(newRequest, newResult)
            } else {
                val newRequest = KollectQuery.Batch(combineIdentities(first, second), ds)
                val newResult = { r: KollectStatus ->
                    when (r) {
                        is KollectStatus.KollectDone<*> -> {
                            r.result as Map<*, *>
                            val xResult = r.result[aId].toOption().map { KollectStatus.KollectDone(it) }.getOrElse { KollectStatus.KollectMissing }
                            val yResult = r.result[anotherId].toOption().map { KollectStatus.KollectDone(it) }.getOrElse { KollectStatus.KollectMissing }
                            IO.applicative().tupled(x.result(xResult), y.result(yResult)).fix().flatMap { IO.unit }
                        }

                        is KollectStatus.KollectMissing ->
                            IO.applicative().tupled(x.result(r), y.result(r)).fix().flatMap { IO.unit }
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
                        IO.applicative().tupled(x.result(oneResult), y.result(r)).fix().flatMap { IO.unit }
                    }
                    is KollectStatus.KollectMissing -> IO.applicative().tupled(x.result(r), y.result(r)).fix().flatMap { IO.unit }
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
                        IO.applicative().tupled(x.result(r), y.result(oneResult)).fix().flatMap { IO.unit }
                    }
                    is KollectStatus.KollectMissing -> IO.applicative().tupled(x.result(r), y.result(r)).fix().flatMap { IO.unit }
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

// `Kollect` result data type
sealed class KollectResult<A> {
    data class Done<A>(val x: A) : KollectResult<A>()
    data class Blocked<A>(val rs: RequestMap, val cont: Kollect<A>) : KollectResult<A>()
    data class Throw<A>(val e: (Env) -> KollectException) : KollectResult<A>()
}

// Kollect data type
@higherkind
sealed class Kollect<A> : KollectOf<A> {
    abstract val run: IO<KollectResult<A>>

    data class Unkollect<A>(override val run: IO<KollectResult<A>>) : Kollect<A>()

    companion object {
        /**
         * Lift a plain value to the Kollect monad.
         */
        fun <A> pure(a: A): Kollect<A> = Unkollect(IO.just(KollectResult.Done(a)))

        fun <A> exception(e: (Env) -> KollectException): Kollect<A> = Unkollect(IO.just(KollectResult.Throw(e)))

        fun <A> error(e: Throwable): Kollect<A> = exception { env -> KollectException.UnhandledException(e, env) }

        fun <I : Any, A> apply(CS: ContextShift<ForIO>, id: I, ds: DataSource<I, A>): Kollect<A> =
            Unkollect(IO.monad().binding {
                val deferred = DeferredK<ForIO, KollectStatus>()
                /*request = FetchOne[I, A](id, ds)
                result = deferred.complete _
                    blocked = BlockedRequest(request, result)
                anyDs = ds.asInstanceOf[DataSource[Any, Any]]
                blockedRequest = RequestMap(Map(anyDs -> blocked))
                for {

                } yield Blocked(blockedRequest, Unfetch(
                    deferred.get.flatMap(_ match {
                        case FetchDone(a: A) =>
                        IO.pure(Done(a))
                        case FetchMissing() =>
                        IO.pure(Throw((env) => MissingIdentity[I, A](id, request, env)))
                    })
                ))*/
            })
    }
}

// Kollect ops
@instance(Kollect::class)
interface KollectMonad<Identity : Any, Result> : Monad<ForKollect> {
    override fun <A> just(a: A): Kollect<A> = Kollect.Unkollect(IO.just(KollectResult.Done(a)))

    override fun <A, B> Kind<ForKollect, A>.map(f: (A) -> B): Kollect<B> = Kollect.Unkollect(IO.monad().binding {
        val kollect = this@map.fix().run.bind()
        val result = when (kollect) {
            is KollectResult.Done -> KollectResult.Done(f(kollect.x))
            is KollectResult.Blocked -> KollectResult.Blocked(kollect.rs, kollect.cont.map(f))
            is KollectResult.Throw -> KollectResult.Throw(kollect.e)
        }
        result
    }.fix())

    override fun <A, B> Kind<ForKollect, A>.product(fb: Kind<ForKollect, B>): Kollect<Tuple2<A, B>> = Kollect.Unkollect(IO.monad().binding {
        val fab = IO.applicative().tupled(this@product.fix().run, fb.fix().run).bind()
        val first = fab.a
        val second = fab.b
        val result = when {
            first is KollectResult.Throw -> KollectResult.Throw(first.e)
            first is KollectResult.Done && second is KollectResult.Done -> KollectResult.Done(Tuple2(first.x, second.x))
            first is KollectResult.Done && second is KollectResult.Blocked -> KollectResult.Blocked(second.rs, this@product.product(second.cont))
            first is KollectResult.Blocked && second is KollectResult.Done -> KollectResult.Blocked(first.rs, first.cont.product(fb))
            first is KollectResult.Blocked && second is KollectResult.Blocked -> KollectResult.Blocked(combineRequestMaps<Identity, Result>(first.rs, second.rs), first.cont.product(second.cont))
            // second is KollectResult.Throw
            else -> KollectResult.Throw((second as KollectResult.Throw).e)
        }
        result
    }.fix())

    override fun <A, B> tailRecM(a: A, f: (A) -> Kind<ForKollect, Either<A, B>>): Kollect<B> = f(a).flatMap {
        when (it) {
            is Either.Left -> tailRecM(a, f)
            is Either.Right -> just(it.b)
        }
    }

    override fun <A, B> Kind<ForKollect, A>.flatMap(f: (A) -> Kind<ForKollect, B>): Kollect<B> =
        Kollect.Unkollect(IO.monad().binding {
            val kollect = this@flatMap.fix().run.bind()
            val result: Kollect<B> = when {
                kollect is KollectResult.Done -> f(kollect.x).fix()
                kollect is KollectResult.Throw -> Kollect.Unkollect(IO.just(KollectResult.Throw(kollect.e)))
                // kollect is KollectResult.Blocked
                else -> Kollect.Unkollect(IO.just(KollectResult.Blocked((kollect as KollectResult.Blocked).rs, kollect.cont.flatMap(f))))
            }
            result.run.bind()
        }.fix())
}
