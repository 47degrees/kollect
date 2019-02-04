package kollect

import arrow.Kind
import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.core.Tuple2
import arrow.data.NonEmptyList
import arrow.effects.typeclasses.Concurrent

sealed class BatchExecution
object Sequentially : BatchExecution()
object InParallel : BatchExecution()

/**
 * A `DataSource` is the recipe for fetching a certain identity `I`, which yields results of type `A`.
 */
interface DataSource<I, A> {

    /**
     * Name given to the data source. Defaults to "KollectDataSource:${this.javaClass.simpleName}".
     */
    fun name(): String = "KollectDataSource:${this.javaClass.simpleName}"

    /**
     * Fetches a value from the source of data by its given Identity. Requires an instance of Concurrent.
     *
     * @param id for the fetched item I.
     * @return IO<Option<A>> since this operation represents an IO computation that returns an optional result. In
     * case the result is not found, it'll return None.
     */
    fun <F> fetch(CF: Concurrent<F>, id: I): Kind<F, Option<A>>

    /**
     * Fetch many identities, returning a mapping from identities to results. If an
     * identity wasn't found, it won't appear in the keys.
     */
    fun <F> batch(CF: Concurrent<F>, ids: NonEmptyList<I>): Kind<F, Map<I, A>> = CF.binding {
        val tuples = KollectExecution.parallel(
                CF,
                ids.map { id -> fetch(CF, id).map { v -> Tuple2(id, v) } }
        ).bind()
        val results: List<Tuple2<I, A>> = tuples.collect(
                f = { Tuple2(it.a, (it.b as Some<A>).t) },
                filter = { it.b is Some<A> }
        )
        results.associateBy({ it.a }, { it.b })
    }

    fun maxBatchSize(): Option<Int> = None

    fun batchExecution(): BatchExecution = InParallel
}
