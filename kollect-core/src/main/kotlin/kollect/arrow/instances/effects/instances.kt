package arrow.effects

import arrow.Kind
import arrow.typeclasses.MonadError
import kollect.KollectMonadError
import kollect.Query

object IOKollectMonadErrorInstance : KollectMonadError<ForIO> {
    override fun ME(): MonadError<ForIO, Throwable> = IO.monadError()

    override fun <A> runQuery(q: Query<A>): Kind<ForIO, A> =
        when(q) {
            is kollect.Sync -> IO.eval(q.action)
            is kollect.Async -> IO.async<A> { callback ->
                TODO()
            }
            is kollect.Ap<*, *> ->
                IO.applicative().product(runQuery(q.fa), runQuery(q.ff)).fix().map { tuple2 -> tuple2.b(tuple2.a) }.hk
        }
}

object IOKollectMonadErrorInstanceImplicits {
    fun instance(): IOKollectMonadErrorInstance = IOKollectMonadErrorInstance
}