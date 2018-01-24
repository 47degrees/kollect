package kollect

///**
// * An environment that is passed along during the fetch rounds. It holds the
// * cache and the list of rounds that have been executed.
// */
//interface Env {
//    fun rounds(): List<Round>
//    fun cache(): DataSourceCache
//    fun evolve(newRound: Round, newCache: DataSourceCache): Env
//}
//
///**
// * A data structure that holds information about a fetch round.
// */
//data class Round(
//        val cache: DataSourceCache,
//        val request: FetchRequest,
//        val response: Any,
//        val start: Long,
//        val end: Long
//) {
//    fun duration(): Double = (end - start) / 1e6
//}
//
///**
// * A concrete implementation of `Env` used in the funault Fetch interpreter.
// */
//data class FetchEnv(
//        val cache: DataSourceCache,
//        val rounds: Queue<Round> = Queue.empty
//): Env {
//
//    override fun evolve(
//            newRound: Round,
//            newCache: DataSourceCache
//    ): FetchEnv =
//            copy(rounds = rounds : + newRound, cache = newCache)
//}
