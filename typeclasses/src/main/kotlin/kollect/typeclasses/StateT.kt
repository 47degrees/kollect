package kollect.typeclasses

import arrow.Kind
import arrow.core.Tuple2
import arrow.data.StateT
import arrow.typeclasses.Applicative

fun <F, S, A> stateT(AF: Applicative<F>, fa: Kind<F, A>): StateT<F, S, A> =
        StateT(AF) { s -> AF.run { fa.map { a -> Tuple2(s, a) } } }