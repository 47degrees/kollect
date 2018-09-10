package arrow.core

import arrow.Kind
import arrow.instance
import arrow.instances.TryMonadInstance
import kollect.KollectMonadError
import kollect.Query

@instance(Try::class)
interface TryKollectMonadErrorInstance : TryMonadInstance, KollectMonadError<ForTry> {

    override fun <A> raiseError(e: Throwable): Try<A> =
        Failure(e)

    override fun <A> Kind<ForTry, A>.handleErrorWith(f: (Throwable) -> Kind<ForTry, A>): Try<A> =
        fix().recoverWith { f(it).fix() }

    override fun <A> runQuery(q: Query<A>): Kind<ForTry, A> = TODO()
}
