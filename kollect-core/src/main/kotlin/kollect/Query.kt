package kollect

import arrow.core.*
import arrow.deriving
import arrow.higherkind
import arrow.typeclasses.Applicative
import java.time.Duration

val Infinite = Duration.ofDays(Long.MAX_VALUE)

typealias QueryCallback<A> = (A) -> Unit
typealias QueryErrorback = (Throwable) -> Unit

@higherkind
@deriving(Applicative::class)
sealed class Query<A> : QueryKind<A> {

    fun <B> ap(ff: QueryKind<(A) -> B>): Query<B> =
        Ap(this, ff.ev())

    companion object {

        fun <A> pure(x: A): Query<A> = Sync(Eval.now(x))

        fun <A> eval(e: Eval<A>): Query<A> = Sync(e)

        fun <A> sync(f: () -> A): Query<A> = Sync(Eval.later(f))

        fun <A> async(action: (QueryCallback<A>, QueryErrorback) -> Unit,
                      timeout: Duration = Infinite): Query<A> = Async(action, timeout)

    }
}

/** A query that can be satisfied synchronously. **/
data class Sync<A>(val action: Eval<A>) : Query<A>()

/** A query that can only be satisfied asynchronously. **/
data class Async<A>(
        val action: (QueryCallback<A>, QueryErrorback) -> Unit,
        val timeout: Duration) : Query<A>()

data class Ap<A, B>(val fa: Query<A>, val ff: Query<(A) -> B>) : Query<B>()