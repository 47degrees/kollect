package kollect

import arrow.Kind
import arrow.core.PartialFunction
import arrow.core.andThen
import arrow.core.invokeOrElse
import arrow.data.NonEmptyList
import arrow.data.extensions.nonemptylist.foldable.traverse_
import arrow.effects.typeclasses.Concurrent
import arrow.typeclasses.ApplicativeError
import kotlinx.coroutines.Dispatchers

internal object KollectExecution {

    fun <F, A> parallel(CF: Concurrent<F>, effects: NonEmptyList<Kind<F, A>>): Kind<F, NonEmptyList<A>> = CF.run {
        effects.traverse(CF) { a -> a.startF(Dispatchers.IO) }.flatMap { fibers ->
            fibers.traverse(CF) { it.join() }.onError(
                    CF, PartialFunction({ true }, { fibers.traverse_(CF) { fiber -> fiber.cancel() } }))
        }
    }
}

fun <F, E, A> Kind<F, A>.onError(AF: ApplicativeError<F, E>, pf: PartialFunction<E, Kind<F, Unit>>): Kind<F, A> =
        AF.onError(this, pf)

fun <F, E, A> ApplicativeError<F, E>.onError(fa: Kind<F, A>, pf: PartialFunction<E, Kind<F, Unit>>): Kind<F, A> =
        fa.handleErrorWith { e ->
            (pf.andThen { fu: Kind<F, Unit> -> fu.map2(raiseError<A>(e)) { (_, b) -> b } }).invokeOrElse(e, ::raiseError)
        }
