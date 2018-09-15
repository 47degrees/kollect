package kollect

import arrow.Kind
import arrow.core.Option
import arrow.core.Tuple2
import arrow.core.toOption
import arrow.data.ListK
import arrow.data.foldable
import arrow.data.k
import arrow.effects.Concurrent
import arrow.instance
import arrow.typeclasses.Monoid
import kollect.arrow.foldLeftM

class DataSourceName(val name: String) : Any()
class DataSourceId(val id: Any) : Any()
class DataSourceResult(val result: Any) : Any()

/**
 *  Users of Kollect can provide their own cache by implementing this interface.
 */
interface DataSourceCache {

    fun <F, Identity : Any, Result : Any> lookup(CF: Concurrent<F>, i: Identity, ds: DataSource<Identity, Result>): Kind<F, Option<Result>>

    fun <F, Identity : Any, Result : Any> insert(CF: Concurrent<F>, i: Identity, v: Result, ds: DataSource<Identity, Result>): Kind<F, DataSourceCache>

    fun <F, Identity : Any, Result : Any> insertMany(
        CF: Concurrent<F>,
        vs: Map<Identity, Result>,
        ds: DataSource<Identity, Result>
    ): Kind<F, DataSourceCache> =
        ListK.foldable().foldLeftM(CF, vs.toList().k(), this) { c, pair: Pair<Identity, Result> ->
            c.insert(CF, pair.first, pair.second, ds)
        }
}

/**
 * A cache that stores its elements in memory.
 */
data class InMemoryCache(
    val state: Map<Tuple2<DataSourceName, DataSourceId>, DataSourceResult>
) : DataSourceCache {

    override fun <F, Identity : Any, Result : Any> lookup(
        CF: Concurrent<F>,
        i: Identity,
        ds: DataSource<Identity, Result>
    ): Kind<F, Option<Result>> = CF.run {
        just(state[Tuple2(DataSourceName(ds.name()), DataSourceId(i))].toOption().map { it.result as Result })
    }

    override fun <F, Identity : Any, Result : Any> insert(
        CF: Concurrent<F>,
        i: Identity,
        v: Result,
        ds: DataSource<Identity, Result>
    ): Kind<F, DataSourceCache> =
        CF.just(copy(state = state.updated(Tuple2(DataSourceName(ds.name()), DataSourceId(i)), DataSourceResult(v))))


    companion object {

        fun empty(): InMemoryCache = InMemoryCache(emptyMap())

        operator fun <Identity : Any, Result : Any> invoke(
            vararg results: Tuple2<Tuple2<String, Identity>, Result>
        ): InMemoryCache = InMemoryCache(results.fold(emptyMap()) { acc, tuple2 ->
            val s = tuple2.a.a
            val i = tuple2.a.b
            val v = tuple2.b
            acc.updated(Tuple2(DataSourceName(s), DataSourceId(i)), DataSourceResult(v))
        })
    }
}

@instance(InMemoryCache::class)
interface inMemoryCacheMonoid : Monoid<InMemoryCache> {
    override fun empty(): InMemoryCache = InMemoryCache.empty()

    override fun InMemoryCache.combine(b: InMemoryCache): InMemoryCache = InMemoryCache(state + b.state)
}

fun <K, V> Map<K, V>.updated(k: K, newVal: V) = this.filterNot { it.key == k } + mapOf(k to newVal)