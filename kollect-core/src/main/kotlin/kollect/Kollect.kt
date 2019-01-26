@file:Suppress("FunctionName", "UNCHECKED_CAST")

package kollect

import arrow.Kind
import arrow.core.None
import arrow.core.Option
import arrow.core.PartialFunction
import arrow.core.Some
import arrow.core.Tuple2
import arrow.data.NonEmptyList
import arrow.effects.Promise
import arrow.effects.Ref
import arrow.effects.typeclasses.Concurrent
import arrow.higherkind
import kollect.arrow.typeclass.Timer
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
    fun <F, A> just(CF: Concurrent<F>, a: A): Kollect<F, A> = Unkollect(CF.just(KollectResult.Done(a)))

    fun <F, A> exception(CF: Concurrent<F>, e: (Env) -> KollectException): Kollect<F, A> = Unkollect(CF.just(KollectResult.Throw(e)))

    fun <F, A> error(CF: Concurrent<F>, e: Throwable): Kollect<F, A> = exception(CF) { env ->
      KollectException.UnhandledException(e, env)
    }

    operator fun <F, I, A> invoke(CF: Concurrent<F>, id: I, ds: DataSource<I, A>): Kollect<F, A> =
      Unkollect(CF.binding {
        val deferred = Promise.unsafeCancelable<F, KollectStatus>(CF)
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
        val deferred = Promise.unsafeCancelable<F, KollectStatus>(CF)
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

    /**
     * Run a [Kollect], the result in the [F] monad.
     */
    fun <F> run(): KollectRunner<F> = KollectRunner()

    class KollectRunner<F> : Any() {
      operator fun <A> invoke(
        CF: Concurrent<F>,
        TF: Timer<F>,
        fa: Kollect<F, A>
      ): Kind<F, A> =
        invoke(CF, TF, fa, InMemoryCache.empty(CF))

      operator fun <A> invoke(
        CF: Concurrent<F>,
        TF: Timer<F>,
        fa: Kollect<F, A>,
        cache: DataSourceCache<F>
      ): Kind<F, A> = CF.binding {
        val cacheRef = Ref.of(cache, CF).bind()
        val result = performRun(CF, TF, fa, cacheRef, None).bind()
        result
      }
    }

    /**
     * Run a `Fetch`, the environment and the result in the `F` monad.
     */
    fun <F> runEnv(): KollectRunnerEnv<F> = KollectRunnerEnv()

    class KollectRunnerEnv<F> : Any() {
      operator fun <A> invoke(
        CF: Concurrent<F>,
        TF: Timer<F>,
        fa: Kollect<F, A>
      ): Kind<F, Tuple2<Env, A>> =
        invoke(CF, TF, fa, InMemoryCache.empty(CF))

      operator fun <A> invoke(
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
    }

    /**
     * Run a `Fetch`, the cache and the result in the `F` monad.
     */
    fun <F> runCache(): KollectRunnerCache<F> = KollectRunnerCache()

    class KollectRunnerCache<F> : Any() {
      operator fun <A> invoke(
        CF: Concurrent<F>,
        TF: Timer<F>,
        fa: Kollect<F, A>
      ): Kind<F, Tuple2<DataSourceCache<F>, A>> =
        invoke(CF, TF, fa, InMemoryCache.empty(CF))

      operator fun <A> invoke(
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
          val requests =
            KollectExecution.parallel(CF, NonEmptyList.fromListUnsafe(blockedRequests).map {
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
