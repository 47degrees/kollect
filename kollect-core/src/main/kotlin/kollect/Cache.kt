package kollect

import arrow.core.Option
import arrow.core.Tuple2
import arrow.core.toOption
import arrow.data.ListK
import arrow.data.foldable
import arrow.data.k
import arrow.effects.IO
import arrow.effects.fix
import arrow.effects.monad
import kollect.arrow.foldLeftM

class DataSourceName(val name: String)
class DataSourceId(val id: Any)

/**
 *  Users of Kollect can provide their own cache by implementing this interface.
 */
interface DataSourceCache<Identity : Any, Result> {

    fun lookup(i: Identity, ds: DataSource<Identity, Result>): IO<Option<Result>>

    fun insert(i: Identity, v: Result, ds: DataSource<Identity, Result>): IO<DataSourceCache<Identity, Result>>

    fun insertMany(vs: Map<Identity, Result>, ds: DataSource<Identity, Result>): IO<DataSourceCache<Identity, Result>> =
        ListK.foldable().foldLeftM(IO.monad(), vs.toList().k(), this) { c, pair: Pair<Identity, Result> ->
            c.insert(pair.first, pair.second, ds)
        }.fix()
}

/**
 * A cache that stores its elements in memory.
 */
data class InMemoryCache<Identity : Any, Result>(
    val state: Map<Tuple2<DataSourceName, DataSourceId>, Result>
) : DataSourceCache<Identity, Result> {

    override fun lookup(i: Identity, ds: DataSource<Identity, Result>): IO<Option<Result>> =
        IO.just(state[Tuple2(DataSourceName(ds.name()), DataSourceId(i))]).map { it.toOption() }

    override fun insert(i: Identity, v: Result, ds: DataSource<Identity, Result>): IO<DataSourceCache<Identity, Result>> =
        IO.just(copy(state = state.filterNot { it.key == Tuple2(DataSourceName(ds.name()), DataSourceId(i)) } +
            mapOf(Tuple2(DataSourceName(ds.name()), DataSourceId(i)) to v)))

    companion object {
        fun <Identity : Any, Result> empty(): InMemoryCache<Identity, Result> = InMemoryCache(emptyMap())

        operator fun <Identity : Any, Result> invoke(
            vararg results: Tuple2<Tuple2<DataSourceName, DataSourceId>, Result>
        ): InMemoryCache<Identity, Result> = InMemoryCache(results.fold(emptyMap()) { map, tuple2 ->
            map.plus(Pair(tuple2.a, tuple2.b))
        })
    }
}