@file:Suppress("FunctionName")

package kollect.arrow

import arrow.Kind
import arrow.core.Either
import arrow.data.Kleisli
import arrow.data.KleisliPartialOf
import arrow.data.fix
import arrow.instance
import arrow.instances.KleisliMonadErrorInstance
import arrow.typeclasses.MonadError
import kollect.arrow.ExitCase.Error

sealed class ExitCase<out E> {

    object Completed : ExitCase<Nothing>()

    object Cancelled : ExitCase<Nothing>()

    data class Error<out E>(val e: E) : ExitCase<E>()
}

fun <E> Either<E, *>.toExitCase() =
    fold(::Error) { ExitCase.Completed }

interface Bracket<F, E> : MonadError<F, E> {

    fun <A, B> Kind<F, A>.bracketCase(release: (A, ExitCase<E>) -> Kind<F, Unit>, use: (A) -> Kind<F, B>): Kind<F, B>

    fun <A, B> Kind<F, A>.bracket(release: (A) -> Kind<F, Unit>, use: (A) -> Kind<F, B>): Kind<F, B> =
        bracketCase({ a, _ -> release(a) }, use)

    fun <A> Kind<F, A>.uncancelable(): Kind<F, A> =
        bracket({ just<Unit>(Unit) }, { just(it) })

    fun <A> Kind<F, A>.guarantee(finalizer: Kind<F, Unit>): Kind<F, A> =
        bracket({ _ -> finalizer }, { _ -> this })

    fun <A> Kind<F, A>.guaranteeCase(finalizer: (ExitCase<E>) -> Kind<F, Unit>): Kind<F, A> =
        bracketCase({ _, e -> finalizer(e) }, { _ -> this })
}

@instance(Bracket::class)
interface KleisliBracket<F, R, E> : Bracket<KleisliPartialOf<F, R>, E> {

    fun BFE(): Bracket<F, E>

    fun KME(): KleisliMonadErrorInstance<F, R, E>

    override fun <A> just(a: A): Kleisli<F, R, A> =
        KME().just(a)

    override fun <A> Kind<KleisliPartialOf<F, R>, A>.handleErrorWith(f: (E) -> Kind<KleisliPartialOf<F, R>, A>): Kleisli<F, R, A> =
        KME().run {
            this@handleErrorWith.handleErrorWith(f)
        }

    override fun <A> raiseError(e: E): Kind<KleisliPartialOf<F, R>, A> = KME().raiseError(e)

    override fun <A, B> Kind<KleisliPartialOf<F, R>, A>.flatMap(f: (A) -> Kind<KleisliPartialOf<F, R>, B>): Kleisli<F, R, B> =
        KME().run {
            this@flatMap.flatMap(f)
        }

    override fun <A, B> tailRecM(a: A, f: (A) -> Kind<KleisliPartialOf<F, R>, Either<A, B>>): Kleisli<F, R, B> =
        KME().tailRecM(a, f)

    override fun <A, B> Kind<KleisliPartialOf<F, R>, A>.bracketCase(
        release: (A, ExitCase<E>) -> Kind<KleisliPartialOf<F, R>, Unit>,
        use: (A) -> Kind<KleisliPartialOf<F, R>, B>): Kleisli<F, R, B> = BFE().run {
        Kleisli { r ->
            this@bracketCase.fix().run(r).bracketCase({ a, br ->
                release(a, br).fix().run(r)
            }, { a ->
                use(a).fix().run(r)
            })
        }
    }

    override fun <A> Kind<KleisliPartialOf<F, R>, A>.uncancelable(): Kleisli<F, R, A> =
        Kleisli { r -> BFE().run { this@uncancelable.fix().run(r).uncancelable() } }
}
