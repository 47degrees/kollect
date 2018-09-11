package kollect.arrow

import arrow.typeclasses.Foldable
import arrow.typeclasses.Monad

fun <F, G, A, B> Foldable<F>.foldLeftM(MG: Monad<G>, fa: arrow.Kind<F, A>, z: B, f: (B, A) -> arrow.Kind<G, B>): arrow.Kind<G, B> =
    fa.foldM(MG, z, f)
