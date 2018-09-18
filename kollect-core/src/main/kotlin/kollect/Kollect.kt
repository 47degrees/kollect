@file:Suppress("FunctionName", "UNCHECKED_CAST")

package kollect

import arrow.Kind
import arrow.core.None
import arrow.core.Option
import arrow.core.PartialFunction
import arrow.core.Some
import arrow.core.Tuple2
import arrow.data.ForNonEmptyList
import arrow.data.NonEmptyList
import arrow.effects.Concurrent
import arrow.effects.deferred.Deferred
import arrow.higherkind
import arrow.typeclasses.Applicative
import arrow.typeclasses.Traverse
import arrow.typeclasses.binding
import kollect.arrow.ContextShift
import kollect.arrow.Par
import kollect.arrow.Parallel.Companion.parTraverse
import kollect.arrow.collect
import kollect.arrow.concurrent.Ref
import kollect.arrow.effects.Timer
import java.util.concurrent.TimeUnit

// Kollect data type
@higherkind
sealed class Kollect<F, A> : KollectOf<F, A> {

    abstract val run: arrow.Kind<F, KollectResult<F, A>>

    data class Unkollect<F, A>(override val run: arrow.Kind<F, KollectResult<F, A>>) : Kollect<F, A>()

    companion object {
        /**
         * Lift a plain value to the Kollect monad.
         */
        fun <F, A> just(AF: Applicative<F>, a: A): Kollect<F, A> = Unkollect(AF.just(KollectResult.Done(a)))

        fun <F, A> exception(AF: Applicative<F>, e: (Env) -> KollectException): Kollect<F, A> = Unkollect(AF.just(KollectResult.Throw(e)))

        fun <F, A> error(AF: Applicative<F>, e: Throwable): Kollect<F, A> = exception(AF) { env -> KollectException.UnhandledException(e, env) }

        operator fun <F, I, A> invoke(AF: Concurrent<F>, id: I, ds: DataSource<I, A>): Kollect<F, A> =
            Unkollect(AF.binding {
                val deferred = Deferred<F, KollectStatus>(AF) as Deferred<F, KollectStatus>
                val request = KollectQuery.KollectOne(id, ds)
                val result = { a: KollectStatus -> deferred.complete(a) }
                val blocked = BlockedRequest(request, result)
                val anyDs = ds as DataSource<Any, Any>
                val blockedRequest = RequestMap(mapOf(anyDs to blocked))

                KollectResult.Blocked(blockedRequest, Unkollect(
                    deferred.get().map {
                        when (it) {
                            is KollectStatus.KollectDone<*> -> KollectResult.Done<F, A>(it.result as A)
                            is KollectStatus.KollectMissing -> KollectResult.Throw<F, A> { env ->
                                KollectException.MissingIdentity(id, request, env)
                            }
                        }
                    }
                ))
            })

        /**
         * Run a `Kollect`, the result in the `F` monad.
         */
        fun <F> run(): KollectRunner<F> = KollectRunner()

        class KollectRunner<F> : Any() {
            operator fun <M, A> invoke(
                TT: Traverse<ForNonEmptyList>,
                P: Par<F, M>,
                C: Concurrent<F>,
                CS: ContextShift<F>,
                TF: Timer<F>,
                fa: Kollect<F, A>,
                cache: DataSourceCache = InMemoryCache.empty()
            ): Kind<F, A> = C.binding {
                val cacheRef = Ref.of(C, cache).bind()
                val result = performRun(TT, P, C, CS, TF, fa, cacheRef, None).bind()
                result
            }
        }

        /**
         * Run a `Fetch`, the environment and the result in the `F` monad.
         */
        fun <F> runEnv(): KollectRunnerEnv<F> = KollectRunnerEnv()

        class KollectRunnerEnv<F> : Any() {
            operator fun <M, A> invoke(
                TT: Traverse<ForNonEmptyList>,
                P: Par<F, M>,
                C: Concurrent<F>,
                CS: ContextShift<F>,
                TF: Timer<F>,
                fa: Kollect<F, A>,
                cache: DataSourceCache = InMemoryCache.empty()
            ): Kind<F, Tuple2<Env, A>> = C.binding {
                val env = Ref.of<F, Env>(C, KollectEnv()).bind()
                val cacheRef = Ref.of(C, cache).bind()
                val result = performRun(TT, P, C, CS, TF, fa, cacheRef, Some(env)).bind()
                val e = env.get().bind()

                Tuple2(e, result)
            }
        }

        /**
         * Run a `Fetch`, the cache and the result in the `F` monad.
         */
        fun <F> runCache(): KollectRunnerCache<F> = KollectRunnerCache()

        class KollectRunnerCache<F> : Any() {
            operator fun <M, A> invoke(
                TT: Traverse<ForNonEmptyList>,
                P: Par<F, M>,
                C: Concurrent<F>,
                CS: ContextShift<F>,
                TF: Timer<F>,
                fa: Kollect<F, A>,
                cache: DataSourceCache = InMemoryCache.empty()
            ): Kind<F, Tuple2<DataSourceCache, A>> = C.binding {
                val cacheRef = Ref.of(C, cache).bind()
                val result = performRun(TT, P, C, CS, TF, fa, cacheRef, None).bind()
                val c = cacheRef.get().bind()

                Tuple2(c, result)
            }
        }

        // Data fetching

        private fun <M, F, A> performRun(
            TT: Traverse<ForNonEmptyList>,
            P: Par<F, M>,
            C: Concurrent<F>,
            CS: ContextShift<F>,
            TF: Timer<F>,
            fa: Kollect<F, A>,
            cache: Ref<F, DataSourceCache>,
            env: Option<Ref<F, Env>>
        ): Kind<F, A> = C.binding {
            val result = fa.run.bind()
            val value = when (result) {
                is KollectResult.Done -> C.just(result.x)
                is KollectResult.Blocked -> binding {
                    fetchRound(TT, P, C, TF, result.rs, cache, env).bind()
                    performRun(TT, P, C, CS, TF, result.cont, cache, env).bind()
                }
                is KollectResult.Throw -> env.fold({
                    C.just(KollectEnv())
                }, {
                    it.get()
                }).flatMap { e: Env ->
                    C.raiseError<A>(result.e(e).toThrowable())
                }
            }.bind()
            value
        }

        private fun <M, F> fetchRound(
            TT: Traverse<ForNonEmptyList>,
            P: Par<F, M>,
            C: Concurrent<F>,
            TF: Timer<F>,
            rs: RequestMap<F>,
            cache: Ref<F, DataSourceCache>,
            env: Option<Ref<F, Env>>
        ): Kind<F, Unit> {
            val blockedRequests = rs.m.toList().map { it.second }
            return if (blockedRequests.isEmpty()) {
                C.just(Unit)
            } else {
                C.binding {
                    val requests =
                        parTraverse(P.parallel(), TT, NonEmptyList.fromListUnsafe(blockedRequests)) {
                            runBlockedRequest(TT, P, C, TF, it, cache)
                        }.bind()

                    val performedRequests = (requests as NonEmptyList<List<Request>>)
                        .foldLeft(listOf<Request>()) { acc, list -> acc + list }
                    if (performedRequests.isEmpty()) {
                        C.just(Unit)
                    } else {
                        when (env) {
                            is Some -> env.t.modify { oldE -> Tuple2(oldE.evolve(Round(performedRequests)), oldE) }
                            is None -> C.just(Unit)
                        }
                    }.bind()
                    Unit
                }
            }
        }

        private fun <M, F> runBlockedRequest(
            TT: Traverse<ForNonEmptyList>,
            P: Par<F, M>,
            C: Concurrent<F>,
            TF: Timer<F>,
            blocked: BlockedRequest<F>,
            cache: Ref<F, DataSourceCache>
        ): Kind<F, List<Request>> =
            blocked.request.let { request ->
                when (request) {
                    is KollectQuery.KollectOne<*, *> -> runKollectOne(C, TF, request as KollectQuery.KollectOne<Any, Any>, blocked.result, cache)
                    else -> runBatch(TT, P, C, TF, request as KollectQuery.Batch<Any, Any>, blocked.result, cache)
                }
            }

        private fun <F> runKollectOne(
            C: Concurrent<F>,
            TF: Timer<F>,
            q: KollectQuery.KollectOne<Any, Any>,
            putResult: (KollectStatus) -> Kind<F, Unit>,
            cache: Ref<F, DataSourceCache>
        ): Kind<F, List<Request>> = C.binding {
            val c = cache.get().bind()
            val maybeCached = c.lookup(C, q.id, q.ds).bind()
            val result = when (maybeCached) {
                // Cached
                is Some -> putResult(KollectStatus.KollectDone(maybeCached.t)).`as`(listOf())
                is None -> binding {
                    val startTime = TF.clock().monotonic(TimeUnit.MILLISECONDS).bind()
                    val o = q.ds.fetch(C, q.id).bind()
                    val endTime = TF.clock().monotonic(TimeUnit.MILLISECONDS).bind()
                    val res = when (o) {
                        // Kollected
                        is Some -> binding {
                            val newC = c.insert(C, q.id, o.t, q.ds).bind()
                            cache.set(newC).bind()
                            putResult(KollectStatus.KollectDone(o.t)).bind()
                            listOf(Request(q, startTime, endTime))
                        }
                        is None -> putResult(KollectStatus.KollectMissing).`as`(listOf(Request(q, startTime, endTime)))
                    }.bind()
                    res
                }
            }.bind()
            result
        }

        private data class BatchedRequest(val batches: List<KollectQuery.Batch<Any, Any>>, val results: Map<Any, Any>)

        private fun <F, M> runBatch(
            TT: Traverse<ForNonEmptyList>,
            P: Par<F, M>,
            C: Concurrent<F>,
            TF: Timer<F>,
            q: KollectQuery.Batch<Any, Any>,
            putResult: (KollectStatus) -> Kind<F, Unit>,
            cache: Ref<F, DataSourceCache>
        ): Kind<F, List<Request>> = C.binding {
            val c = cache.get().bind()

            // Remove cached IDs
            val idLookups = q.ids.traverse(C) { i ->
                c.lookup(C, i, q.ds).tupleLeft(i)
            }.bind()

            val cachedResults = idLookups.collect<Tuple2<Any, Option<Any>>, Pair<Any, Any>>(PartialFunction(
                definedAt = { it.b is Some },
                ifDefined = { Pair(it.a, (it.b as Some).t) }
            )).toMap()

            val uncachedIds = idLookups.collect<Tuple2<Any, Option<Any>>, Any>(PartialFunction(
                definedAt = { it.b is None },
                ifDefined = { it.a }
            ))

            val result = when {
                // All cached
                uncachedIds.isEmpty() -> putResult(KollectStatus.KollectDone(cachedResults)).flatMap { C.just(listOf<Request>()) }

                // Some uncached
                else -> binding {
                    val startTime = TF.clock().monotonic(TimeUnit.MILLISECONDS).bind()

                    val uncached = NonEmptyList.fromListUnsafe(uncachedIds)
                    val request = KollectQuery.Batch(uncached, q.ds)

                    val batchedRequest = request.ds.maxBatchSize().let { maxBatchSize ->
                        when (maxBatchSize) {
                            // Unbatched
                            is None -> request.ds.batch(TT, C, P, uncached).map {
                                BatchedRequest(listOf(request), it)
                            }
                            // Batched
                            is Some -> runBatchedRequest(TT, P, C, request, maxBatchSize.t, request.ds.batchExecution())
                        }
                    }.bind()

                    val endTime = TF.clock().monotonic(TimeUnit.MILLISECONDS).bind()
                    val resultMap = combineBatchResults(batchedRequest.results, cachedResults)
                    val updatedCache = c.insertMany(C, batchedRequest.results, request.ds).bind()

                    cache.set(updatedCache).bind()
                    putResult(KollectStatus.KollectDone(resultMap)).bind()
                    batchedRequest.batches.map { Request(it, startTime, endTime) }
                }
            }.bind()

            result
        }

        private fun <F, M> runBatchedRequest(
            TT: Traverse<ForNonEmptyList>,
            P: Par<F, M>,
            C: Concurrent<F>,
            q: KollectQuery.Batch<Any, Any>,
            batchSize: Int,
            e: BatchExecution
        ): Kind<F, BatchedRequest> {
            val batches = NonEmptyList.fromListUnsafe(q.ids.all.chunked(batchSize).map { batchIds ->
                NonEmptyList.fromListUnsafe(batchIds)
            }.toList())

            val requests = batches.all.map { KollectQuery.Batch(it, q.ds) }

            val results = when (e) {
                is Sequentially -> batches.traverse(C) { q.ds.batch(TT, C, P, it) }
                is InParallel -> parTraverse(P.parallel(), TT, batches) { q.ds.batch(TT, C, P, it) }
            }

            return C.run {
                results.map {
                    (it as NonEmptyList<Map<Any, Any>>).all.reduce(::combineBatchResults)
                }.map { BatchedRequest(requests, it) }
            }
        }

        private fun combineBatchResults(r: Map<Any, Any>, rs: Map<Any, Any>): Map<Any, Any> = r + rs
    }
}
