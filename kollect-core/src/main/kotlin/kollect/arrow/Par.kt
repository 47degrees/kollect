package kollect.arrow

import kollect.arrow.typeclass.Parallel

interface Par<F, G> {

    fun parallel(): Parallel<F, G>

    companion object {

        operator fun <F, G, A> invoke(ev: Par<F, G>) = ev

        fun <F, G> fromParallel(P: Parallel<F, G>): Par<F, G> = object : Par<F, G> {
            override fun parallel(): Parallel<F, G> = P
        }
    }
}
