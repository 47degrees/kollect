package kollect.arrow

import arrow.Kind
import arrow.core.Either
import arrow.effects.ForIO
import arrow.effects.IO
import arrow.effects.IOMonadErrorInstance
import arrow.effects.IOOf
import arrow.instance
import arrow.typeclasses.MonadError
import kollect.arrow.ExitCase.Failing

sealed class ExitCase<out E> {

    object Completed : ExitCase<Nothing>()

    object Cancelled : ExitCase<Nothing>()

    data class Failing<out E>(val e: E) : ExitCase<E>()
}

fun <E> Either<E, *>.toExitCase() =
    fold(::Failing) { ExitCase.Completed }

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

@instance(IO::class)
interface IOBracketInstance : IOMonadErrorInstance, Bracket<ForIO, Throwable> {
    override fun <A, B> IOOf<A>.bracketCase(release: (A, ExitCase<Throwable>) -> IOOf<Unit>, use: (A) -> IOOf<B>): IO<B> =
        TODO()
}
