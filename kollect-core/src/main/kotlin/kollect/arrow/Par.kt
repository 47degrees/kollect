package kollect.arrow

interface Par<F, G> {

    fun parallel(): Parallel<F, G>

    companion object {

        operator fun <F, G, A> invoke(ev: Par<F, G>) = ev
        // def apply[F[_]](implicit ev: Par[F]) = ev

        // type Aux[F[_], G[_]] = Par[F]{type ParAux[A] = G[A]}

        fun <F, G, A> fromParallel(P: Parallel<F, G>): Par<F, G> = object : Par<F, G> {
            override fun parallel(): Parallel<F, G> = P
        }
    }
}

//
//trait Par[F[_]]{
//    type ParAux[A]
//    def parallel: Parallel[F, ParAux]
//}
//
//object Par {
//    def apply[F[_]](implicit ev: Par[F]) = ev
//
//    type Aux[F[_], G[_]] = Par[F]{type ParAux[A] = G[A]}
//
//    implicit def fromParallel[F[_], G[_]](implicit P: Parallel[F, G]): Par.Aux[F, G] =
//    new Par[F]{
//        type ParAux[A] = G[A]
//        def parallel: Parallel[F, ParAux] = P
//    }
//
//}