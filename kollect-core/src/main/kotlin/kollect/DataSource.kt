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
import arrow.typeclasses.Traverse
import kollect.arrow.Par
import kollect.arrow.Parallel.Companion.parTraverse
import kollect.arrow.collect

sealed class BatchExecution
object Sequentially : BatchExecution()
object InParallel : BatchExecution()

/**
 * A `DataSource` is the recipe for fetching a certain identity `I`, which yields
 * results of type `A`.
 */
interface DataSource<Identity, Result> {
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
    fun <F> fetch(CF: Concurrent<F>, id: Identity): Kind<F, Option<Result>>

    @Suppress("UNCHECKED_CAST")
    fun <F, G> batch(
        TT: Traverse<ForNonEmptyList>,
        CF: Concurrent<F>,
        P: Par<F, G>,
        ids: NonEmptyList<Identity>
    ): Kind<F, Map<Identity, Result>> = CF.run {
        parTraverse(P.parallel(), TT, ids) { id ->
            fetch(CF, id).map { (it as Kind<F, Result>).tupleLeft(id) }
        }.map {
            it.collect<Kind<F, Tuple2<Identity, Result>>, Pair<Identity, Result>>(PartialFunction(
                definedAt = { value -> value is Some<*> },
                ifDefined = { value -> (value as Some<Pair<Identity, Result>>).t }
            )).toMap()
        }
    }

    fun maxBatchSize(): Option<Int> = None

    fun batchExecution(): BatchExecution = InParallel
}
