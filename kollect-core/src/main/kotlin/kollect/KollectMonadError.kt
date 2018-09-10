package kollect

import arrow.Kind
import arrow.typeclasses.MonadError

interface KollectMonadError<F> : MonadError<F, Throwable> {

    fun <A> runQuery(q: Query<A>): Kind<F, A>
}