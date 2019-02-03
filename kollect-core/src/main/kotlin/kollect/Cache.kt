package kollect

import arrow.Kind
import arrow.core.Option
import arrow.core.Tuple2
import arrow.core.toOption
import arrow.data.extensions.listk.foldable.foldM
import arrow.data.k
import arrow.typeclasses.Monad

inline class DataSourceResult(val result: Any)

/**
 *  Users of Kollect can provide their own cache by implementing this interface.
 */
interface DataSourceCache<F> {

    fun <I : Any, A : Any> lookup(i: I, ds: DataSource<I, A>): Kind<F, Option<A>>

    fun <I : Any, A : Any> insert(i: I, v: A, ds: DataSource<I, A>): Kind<F, DataSourceCache<F>>

    fun <I : Any, A : Any> bulkInsert(
            MF: Monad<F>,
            vs: List<Tuple2<I, A>>,
            ds: DataSource<I, A>): Kind<F, DataSourceCache<F>> = vs.k().foldM(MF, this) { cache, tuple ->
        cache.insert(tuple.a, tuple.b, ds)
    }
}

/**
 * A cache that stores its elements in memory.
 */
data class InMemoryCache<F>(
        val MF: Monad<F>,
        val state: Map<Tuple2<String, Any>, DataSourceResult>
) : DataSourceCache<F>, Monad<F> by MF {

    override fun <I : Any, A : Any> lookup(i: I, ds: DataSource<I, A>): Kind<F, Option<A>> = run {
        just(state[Tuple2(ds.name(), i)].toOption().map { it.result as A })
    }

    override fun <I : Any, A : Any> insert(i: I, v: A, ds: DataSource<I, A>): Kind<F, DataSourceCache<F>> = run {
        just(copy(state = state.updated(Tuple2(ds.name(), i), DataSourceResult(v))))
    }

    companion object {

        fun <F> empty(MF: Monad<F>): InMemoryCache<F> = InMemoryCache(MF, emptyMap())

        operator fun <F, I : Any, A : Any> invoke(
                MF: Monad<F>,
                vararg results: Tuple2<Tuple2<String, I>, A>
        ): InMemoryCache<F> = InMemoryCache(MF, results.fold(emptyMap()) { acc, tuple2 ->
            val s = tuple2.a.a
            val i = tuple2.a.b
            val v = tuple2.b
            acc.updated(Tuple2(s, i), DataSourceResult(v))
        })
    }
}

fun <K, V> Map<K, V>.updated(k: K, newVal: V) = this.filterNot { it.key == k } + mapOf(k to newVal)
