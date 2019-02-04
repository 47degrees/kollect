package kollect

/**
 * An environment that is passed along during the kollect rounds. It holds the cache and the list of rounds that have
 * been executed.
 */
interface Env {
    val rounds: List<Round>
    fun evolve(newRound: Round): Env
}

/**
 * A data structure that holds information about a request inside a kollect round.
 */
data class Request(
        val request: KollectRequest,
        val start: Long,
        val end: Long,
        val duration: Long = end - start
)

/**
 * A data structure that holds information about a kollect round.
 */
data class Round(val queries: List<Request>)

/**
 * A concrete implementation of `Env` used in the default Kollect interpreter.
 */
data class KollectEnv(override val rounds: List<Round> = listOf()) : Env {

    override fun evolve(newRound: Round): KollectEnv = copy(rounds = rounds + newRound)
}
