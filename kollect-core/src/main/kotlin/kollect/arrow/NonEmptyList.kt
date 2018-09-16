package kollect.arrow

import arrow.Kind
import arrow.typeclasses.Traverse
//
///**
// * Like `Traverse<A>.traverse()`, but uses the applicative instance
// * corresponding to the Parallel instance instead.
// */
//fun <T, M, F, A, B> parTraverse(
//    TT: Traverse<T>,
//    P: Parallel<M, F>,
//    ta: Kind<T, A>,
//    f: (A) -> Kind<M, B>
//): Kind<M, Kind<T, B>> {
//    val gtb: Kind<F, Kind<T, B>> = TT.run { ta.traverse(P.applicative()) { P.parallel().invoke(f(it)) } }
//    return P.sequential().invoke(gtb)
//}
