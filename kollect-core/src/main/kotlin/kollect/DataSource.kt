package kollect

import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.core.Tuple2
import arrow.core.fix
import arrow.core.functor
import arrow.effects.IO
import arrow.effects.parallelMapN
import kollect.arrow.extensions.tupleLeft
import kotlinx.coroutines.experimental.CommonPool

class TooMuchConcurrencyException : Exception("It's just supported to pass up to 9 ids for now.")

sealed class BatchExecution
object Sequentially : BatchExecution()
object InParallel : BatchExecution()

/**
 * Result `DataSource` is the recipe for fetching a certain identity `Identity`, which yields
 * results of type `Result`.
 */
interface DataSource<Identity : Any, Result> {

    /**
     * Name given to the data source. It takes the string "KollectDataSource:${this.javaClass.simpleName}" as a default,
     * but can be overriden.
     */
    fun name(): String = "KollectDataSource:${this.javaClass.simpleName}"

    /**
     * Fetches a value from the source of data by its given Identity.
     *
     * @param id for the fetched item Identity.
     * @return IO<Option<Result>> since this operation represents an IO computation that returns an optional result. In
     * case the result is not found, it'll return None.
     */
    fun fetch(id: Identity): IO<Option<Result>>

    private fun fetchOneById(id: Identity): IO<Option<Tuple2<Identity, Result>>> =
        fetch(id).map { Option.functor().tupleLeft(it, id).fix() }

    /**
     * Fetch many identities paralelly, returning a mapping from identities to results. If an identity wasn't found,
     * it won't appear in the keys.
     *
     * @param ids as a List of identities to fetch.
     * @return IO<Map<Identity, Result>> representing an IO operation that will eventually return a map of relations
     * from Id to Result for all the fetched items.
     */
    fun batch(ids: List<Identity>): IO<Map<Identity, Result>> = when (ids.size) {
        0 -> IO.just(mapOf())
        1 -> IO.async { fetchOneById(ids[0]) }
        2 -> IO.parallelMapN(CommonPool, fetchOneById(ids[0]), fetchOneById(ids[1])) { a, b -> a.toMap() + b.toMap() }
        3 -> IO.parallelMapN(CommonPool, fetchOneById(ids[0]), fetchOneById(ids[1]), fetchOneById(ids[2])) { a, b, c ->
            a.toMap() + b.toMap() + c.toMap()
        }
        4 -> IO.parallelMapN(
            CommonPool,
            fetchOneById(ids[0]),
            fetchOneById(ids[1]),
            fetchOneById(ids[2]),
            fetchOneById(ids[3])
        ) { a, b, c, d ->
            a.toMap() + b.toMap() + c.toMap() + d.toMap()
        }
        5 -> IO.parallelMapN(
            CommonPool,
            fetchOneById(ids[0]),
            fetchOneById(ids[1]),
            fetchOneById(ids[2]),
            fetchOneById(ids[3]),
            fetchOneById(ids[4])
        ) { a, b, c, d, e ->
            a.toMap() + b.toMap() + c.toMap() + d.toMap() + e.toMap()
        }
        6 -> IO.parallelMapN(
            CommonPool,
            fetchOneById(ids[0]),
            fetchOneById(ids[1]),
            fetchOneById(ids[2]),
            fetchOneById(ids[3]),
            fetchOneById(ids[4]),
            fetchOneById(ids[5])
        ) { a, b, c, d, e, f ->
            a.toMap() + b.toMap() + c.toMap() + d.toMap() + e.toMap() + f.toMap()
        }
        7 -> IO.parallelMapN(
            CommonPool,
            fetchOneById(ids[0]),
            fetchOneById(ids[1]),
            fetchOneById(ids[2]),
            fetchOneById(ids[3]),
            fetchOneById(ids[4]),
            fetchOneById(ids[5]),
            fetchOneById(ids[6])
        ) { a, b, c, d, e, f, g ->
            a.toMap() + b.toMap() + c.toMap() + d.toMap() + e.toMap() + f.toMap() + g.toMap()
        }
        8 -> IO.parallelMapN(
            CommonPool,
            fetchOneById(ids[0]),
            fetchOneById(ids[1]),
            fetchOneById(ids[2]),
            fetchOneById(ids[3]),
            fetchOneById(ids[4]),
            fetchOneById(ids[5]),
            fetchOneById(ids[6]),
            fetchOneById(ids[7])
        ) { a, b, c, d, e, f, g, h ->
            a.toMap() + b.toMap() + c.toMap() + d.toMap() + e.toMap() + f.toMap() + g.toMap() + h.toMap()
        }
        9 -> IO.parallelMapN(
            CommonPool,
            fetchOneById(ids[0]),
            fetchOneById(ids[1]),
            fetchOneById(ids[2]),
            fetchOneById(ids[3]),
            fetchOneById(ids[4]),
            fetchOneById(ids[5]),
            fetchOneById(ids[6]),
            fetchOneById(ids[7]),
            fetchOneById(ids[8])
        ) { a, b, c, d, e, f, g, h, i ->
            a.toMap() + b.toMap() + c.toMap() + d.toMap() + e.toMap() + f.toMap() + g.toMap() + h.toMap() + i.toMap()
        }
        else -> IO.raiseError(TooMuchConcurrencyException())
    }

    fun <A, B> Option<Tuple2<A, B>>.toMap(): Map<A, B> {
        val option = this
        return when (option) {
            is Some -> mapOf(option.get().a to option.get().b)
            is None -> mapOf()
        }
    }

    fun maxBatchSize(): Option<Int> = None

    fun batchExecution(): BatchExecution = InParallel
}
