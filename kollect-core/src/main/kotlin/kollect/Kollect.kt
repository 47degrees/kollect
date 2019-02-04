@file:Suppress("FunctionName", "UNCHECKED_CAST")

package kollect

import arrow.Kind
import arrow.core.*
import arrow.data.NonEmptyList
import arrow.effects.Promise
import arrow.effects.Ref
import arrow.effects.typeclasses.Concurrent
import arrow.higherkind
import arrow.typeclasses.Applicative
import arrow.typeclasses.Monad
import kollect.typeclasses.Timer
import java.util.concurrent.TimeUnit

@higherkind
sealed class Kollect<F, A> : KollectOf<F, A> {

    abstract val run: arrow.Kind<F, KollectResult<F, A>>

    data class Unkollect<F, A>(override val run: arrow.Kind<F, KollectResult<F, A>>) : Kollect<F, A>()

    fun <B> map(MF: Monad<F>, f: (A) -> B): Kollect<F, B> =
            Kollect.Unkollect(MF.binding {
                val kollect = run.bind()
                val result = when (kollect) {
                    is KollectResult.Done -> KollectResult.Done<F, B>(f(kollect.x))
                    is KollectResult.Blocked -> KollectResult.Blocked(kollect.rs, kollect.cont.map(MF, f))
                    is KollectResult.Throw -> KollectResult.Throw(kollect.e)
                }
                result
            })

    fun <I, B> ap(MF: Monad<F>, ff: Kind<KollectPartialOf<F>, (A) -> B>): Kollect<F, B> =
            product<I, (A) -> B>(MF, ff).map(MF) { tuple -> tuple.b(tuple.a) }

    fun <I, B> product(MF: Monad<F>, fb: Kind<KollectPartialOf<F>, B>): Kollect<F, Tuple2<A, B>> =
            Unkollect(MF.binding {
                val fab = MF.tupled(run, fb.fix().run).bind()
                val first = fab.a
                val second = fab.b
                val result = when {
                    first is KollectResult.Throw ->
                        KollectResult.Throw<F, Tuple2<A, B>>(first.e)
                    first is KollectResult.Done && second is KollectResult.Done ->
                        KollectResult.Done(Tuple2(first.x, second.x))
                    first is KollectResult.Done && second is KollectResult.Blocked ->
                        KollectResult.Blocked(second.rs, product<I, B>(MF, second.cont))
                    first is KollectResult.Blocked && second is KollectResult.Done ->
                        KollectResult.Blocked(first.rs, first.cont.product<I, B>(MF, fb))
                    first is KollectResult.Blocked && second is KollectResult.Blocked ->
                        KollectResult.Blocked(
                                combineRequestMaps<I, A, F>(MF, first.rs, second.rs),
                                first.cont.product<I, B>(MF, second.cont))
                    // second is KollectResult.Throw ->
                    else ->
                        KollectResult.Throw((second as KollectResult.Throw).e)
                }
                result
            })

    fun <B> flatMap(MF: Monad<F>, f: (A) -> Kind<KollectPartialOf<F>, B>): Kollect<F, B> = MF.run {
        Unkollect(run.flatMap {
            when (it) {
                is KollectResult.Done -> f(it.x).fix().run
                is KollectResult.Throw -> MF.just(KollectResult.Throw(it.e))
                is KollectResult.Blocked -> MF.just(KollectResult.Blocked(it.rs, it.cont.flatMap(MF, f)))
            }
        })
    }

    companion object {
        /**
         * Lift a plain value to the Kollect monad.
         */
        fun <F, A> just(AF: Applicative<F>, a: A): Kollect<F, A> = Unkollect(AF.just(KollectResult.Done(a)))

        fun <F, A> exception(AF: Applicative<F>, e: (Env) -> KollectException): Kollect<F, A> = Unkollect(AF.just(KollectResult.Throw(e)))

        fun <F, A> error(AF: Applicative<F>, e: Throwable): Kollect<F, A> = exception(AF) { env ->
            KollectException.UnhandledException(e, env)
        }

        operator fun <F, I, A> invoke(CF: Concurrent<F>, id: I, ds: DataSource<I, A>): Kollect<F, A> =
                Unkollect(CF.binding {
                    val deferred = Promise<F, KollectStatus>(CF).bind()
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

        fun <F, I, A> optional(CF: Concurrent<F>, id: I, ds: DataSource<I, A>): Kollect<F, Option<A>> =
                Unkollect(CF.binding {
                    val deferred = Promise<F, KollectStatus>(CF).bind()
                    val request = KollectQuery.KollectOne(id, ds)
                    val result = { a: KollectStatus -> deferred.complete(a) }
                    val blocked = BlockedRequest(request, result)
                    val anyDs = ds as DataSource<Any, Any>
                    val blockedRequest = RequestMap(mapOf(anyDs to blocked))

                    KollectResult.Blocked(blockedRequest, Unkollect(
                            deferred.get().map {
                                when (it) {
                                    is KollectStatus.KollectDone<*> -> KollectResult.Done<F, Option<A>>(Some(it.result as A))
                                    is KollectStatus.KollectMissing -> KollectResult.Done(Option.empty())
                                }
                            }
                    ))
                })

        fun <F, A, B> tailRecM(MF: Monad<F>, a: A, f: (A) -> Kind<KollectPartialOf<F>, Either<A, B>>): Kollect<F, B> = MF.run {
            f(a).fix().flatMap(MF) {
                when (it) {
                    is Either.Left -> tailRecM(MF, a, f)
                    is Either.Right -> just(MF, it.b)
                }
            }
        }

        /**
         * Runs a [Kollect], returning a result lifted to the [F] context.
         */
        fun <F, A> run(
                CF: Concurrent<F>,
                TF: Timer<F>,
                fa: Kollect<F, A>
        ): Kind<F, A> = run(CF, TF, fa, InMemoryCache.empty(CF))

        /**
         * Runs a [Kollect], returning a result lifted to the [F] context.
         */
        fun <F, A> run(
                CF: Concurrent<F>,
                TF: Timer<F>,
                fa: Kollect<F, A>,
                cache: DataSourceCache<F>
        ): Kind<F, A> = CF.binding {
            val cacheRef = Ref.of(cache, CF).bind()
            val result = performRun(CF, TF, fa, cacheRef, None).bind()
            result
        }

        /**
         * Run a [Kollect], returning the environment and the result in the [F] context.
         */
        fun <F, A> runEnv(
                CF: Concurrent<F>,
                TF: Timer<F>,
                fa: Kollect<F, A>
        ): Kind<F, Tuple2<Env, A>> =
                runEnv(CF, TF, fa, InMemoryCache.empty(CF))

        /**
         * Run a [Kollect], returning the environment and the result in the [F] context.
         */
        fun <F, A> runEnv(
                CF: Concurrent<F>,
                TF: Timer<F>,
                fa: Kollect<F, A>,
                cache: DataSourceCache<F>
        ): Kind<F, Tuple2<Env, A>> = CF.binding {
            val env = Ref.of<F, Env>(KollectEnv(), CF).bind()
            val cacheRef = Ref.of(cache, CF).bind()
            val result = performRun(CF, TF, fa, cacheRef, Some(env)).bind()
            val e = env.get().bind()

            Tuple2(e, result)
        }

        /**
         * Runs a [Kollect], returning the cache and the result in the [F] context.
         */
        fun <F, A> runCache(
                CF: Concurrent<F>,
                TF: Timer<F>,
                fa: Kollect<F, A>
        ): Kind<F, Tuple2<DataSourceCache<F>, A>> =
                runCache(CF, TF, fa, InMemoryCache.empty(CF))

        /**
         * Runs a [Kollect], returning the cache and the result in the [F] context.
         */
        fun <F, A> runCache(
                CF: Concurrent<F>,
                TF: Timer<F>,
                fa: Kollect<F, A>,
                cache: DataSourceCache<F>
        ): Kind<F, Tuple2<DataSourceCache<F>, A>> = CF.binding {
            val cacheRef = Ref.of(cache, CF).bind()
            val result = performRun(CF, TF, fa, cacheRef, None).bind()
            val c = cacheRef.get().bind()

            Tuple2(c, result)
        }

        // Data fetching

        private fun <F, A> performRun(
                CF: Concurrent<F>,
                TF: Timer<F>,
                fa: Kollect<F, A>,
                cache: Ref<F, DataSourceCache<F>>,
                env: Option<Ref<F, Env>>
        ): Kind<F, A> = CF.binding {
            val result = fa.run.bind()
            val value = when (result) {
                is KollectResult.Done -> CF.just(result.x)
                is KollectResult.Blocked -> binding {
                    fetchRound(CF, TF, result.rs, cache, env).bind()
                    performRun(CF, TF, result.cont, cache, env).bind()
                }
                is KollectResult.Throw -> env.fold({
                    CF.just(KollectEnv())
                }, {
                    it.get()
                }).flatMap { e: Env ->
                    CF.raiseError<A>(result.e(e).toThrowable())
                }
            }.bind()
            value
        }

        private fun <F> fetchRound(
                CF: Concurrent<F>,
                TF: Timer<F>,
                rs: RequestMap<F>,
                cache: Ref<F, DataSourceCache<F>>,
                env: Option<Ref<F, Env>>
        ): Kind<F, Unit> {
            val blockedRequests = rs.m.toList().map { it.second }
            return if (blockedRequests.isEmpty()) {
                CF.just(Unit)
            } else {
                CF.binding {
                    val requests = KollectExecution.parallel(CF, NonEmptyList.fromListUnsafe(blockedRequests).map {
                        runBlockedRequest(CF, TF, it, cache, env)
                    }).bind()

                    val performedRequests = requests.foldLeft(listOf<Request>()) { acc, list -> acc + list }
                    if (performedRequests.isEmpty()) {
                        CF.just(Unit)
                    } else {
                        when (env) {
                            is Some -> env.t.modify { oldE -> Tuple2(oldE.evolve(Round(performedRequests)), oldE) }
                            is None -> CF.just(Unit)
                        }
                    }.bind()
                    Unit
                }
            }
        }

        private fun <F> runBlockedRequest(
                CF: Concurrent<F>,
                TF: Timer<F>,
                blocked: BlockedRequest<F>,
                cache: Ref<F, DataSourceCache<F>>,
                env: Option<Ref<F, Env>>
        ): Kind<F, List<Request>> =
                blocked.request.let { request ->
                    when (request) {
                        is KollectQuery.KollectOne<*, *> -> runKollectOne(CF, TF, request as KollectQuery.KollectOne<Any, Any>, blocked.result, cache, env)
                        else -> runBatch(CF, TF, request as KollectQuery.Batch<Any, Any>, blocked.result, cache, env)
                    }
                }

        private fun <F> runKollectOne(
                CF: Concurrent<F>,
                TF: Timer<F>,
                q: KollectQuery.KollectOne<Any, Any>,
                putResult: (KollectStatus) -> Kind<F, Unit>,
                cache: Ref<F, DataSourceCache<F>>,
                env: Option<Ref<F, Env>>
        ): Kind<F, List<Request>> = CF.binding {
            val c = cache.get().bind()
            val maybeCached = c.lookup(q.id, q.ds).bind()
            val result = when (maybeCached) {
                // Cached
                is Some -> putResult(KollectStatus.KollectDone(maybeCached.t)).`as`(listOf())

                // Not cached, must kollect
                is None -> binding {
                    val startTime = TF.clock().monotonic(TimeUnit.MILLISECONDS).bind()
                    val o = q.ds.fetch(CF, q.id).bind()
                    val endTime = TF.clock().monotonic(TimeUnit.MILLISECONDS).bind()
                    val res = when (o) {
                        // Kollected
                        is Some -> binding {
                            val newC = c.insert(q.id, o.t, q.ds).bind()
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

        private fun <F> runBatch(
                CF: Concurrent<F>,
                TF: Timer<F>,
                q: KollectQuery.Batch<Any, Any>,
                putResult: (KollectStatus) -> Kind<F, Unit>,
                cache: Ref<F, DataSourceCache<F>>,
                env: Option<Ref<F, Env>>
        ): Kind<F, List<Request>> = CF.binding {
            val c = cache.get().bind()

            // Remove cached IDs
            val idLookups = q.ids.traverse(CF) { i ->
                c.lookup(i, q.ds).tupleLeft(i)
            }.bind()

            val cachedResults = idLookups.collect(
                    f = { Pair(it.a, (it.b as Some).t) },
                    filter = { it.b is Some }
            ).toMap()

            val uncachedIds = idLookups.collect(
                    f = { it.a },
                    filter = { it.b is None }
            )

            val result = when {
                // All cached
                uncachedIds.isEmpty() -> putResult(KollectStatus.KollectDone(cachedResults)).`as`(listOf<Request>())

                // Some uncached
                else -> binding {
                    val startTime = TF.clock().monotonic(TimeUnit.MILLISECONDS).bind()

                    val uncached = NonEmptyList.fromListUnsafe(uncachedIds)
                    val request = KollectQuery.Batch(uncached, q.ds)

                    val batchedRequest = request.ds.maxBatchSize().let { maxBatchSize ->
                        when (maxBatchSize) {
                            // Unbatched
                            is None -> request.ds.batch(CF, uncached).map {
                                BatchedRequest(listOf(request), it)
                            }
                            // Batched
                            is Some -> runBatchedRequest(CF, request, maxBatchSize.t, request.ds.batchExecution())
                        }
                    }.bind()

                    val endTime = TF.clock().monotonic(TimeUnit.MILLISECONDS).bind()
                    val resultMap = combineBatchResults(batchedRequest.results, cachedResults)
                    val updatedCache = c.bulkInsert(CF, batchedRequest.results.toList().map { Tuple2(it.first, it.second) }, request.ds).bind()

                    cache.set(updatedCache).bind()
                    putResult(KollectStatus.KollectDone(resultMap)).bind()
                    batchedRequest.batches.map { Request(it, startTime, endTime) }
                }
            }.bind()

            result
        }

        private fun <F> runBatchedRequest(
                CF: Concurrent<F>,
                q: KollectQuery.Batch<Any, Any>,
                batchSize: Int,
                e: BatchExecution
        ): Kind<F, BatchedRequest> {
            val batches = NonEmptyList.fromListUnsafe(q.ids.all.chunked(batchSize).map { batchIds ->
                NonEmptyList.fromListUnsafe(batchIds)
            }.toList())

            val requests = batches.all.map { KollectQuery.Batch(it, q.ds) }

            val results = when (e) {
                is Sequentially -> batches.traverse(CF) { q.ds.batch(CF, it) }
                is InParallel -> KollectExecution.parallel(CF, batches.map { q.ds.batch(CF, it) })
            }

            return CF.run {
                results.map { it.all.reduce(::combineBatchResults) }.map { BatchedRequest(requests, it) }
            }
        }

        private fun combineBatchResults(r: Map<Any, Any>, rs: Map<Any, Any>): Map<Any, Any> = r + rs
    }
}
