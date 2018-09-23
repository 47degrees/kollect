@file:Suppress("FunctionName")

package kollect.arrow.instances

import arrow.Kind
import arrow.core.Either
import arrow.core.Tuple2
import arrow.core.andThen
import arrow.core.right
import arrow.core.some
import arrow.data.EitherT
import arrow.data.EitherTPartialOf
import arrow.data.Kleisli
import arrow.data.KleisliPartialOf
import arrow.data.OptionT
import arrow.data.OptionTPartialOf
import arrow.data.StateT
import arrow.data.StateTPartialOf
import arrow.data.WriterT
import arrow.data.WriterTPartialOf
import arrow.data.fix
import arrow.effects.typeclasses.MonadDefer
import arrow.instance
import arrow.instances.EitherTMonadErrorInstance
import arrow.instances.OptionTMonadInstance
import arrow.instances.StateTMonadInstance
import arrow.instances.WriterTApplicativeInstance
import arrow.instances.WriterTMonadInstance
import arrow.typeclasses.ApplicativeError
import arrow.typeclasses.Functor
import arrow.typeclasses.Monad
import arrow.typeclasses.MonadError
import arrow.typeclasses.Monoid

@instance(EitherT::class)
interface EitherTMonadDefer<F, L> : MonadDefer<EitherTPartialOf<F, L>> {

    fun MF(): MonadDefer<F>

    override fun <A> just(a: A): EitherT<F, L, A> = EitherT.just(MF(), a)

    override fun <A> Kind<EitherTPartialOf<F, L>, A>.handleErrorWith(f: (Throwable) -> Kind<EitherTPartialOf<F, L>, A>): EitherT<F, L, A> =
        MF().run {
            EitherT(this@handleErrorWith.fix().value.handleErrorWith(f.andThen { it.fix().value }))
        }

    override fun <A> raiseError(e: Throwable): EitherT<F, L, A> = MF().run {
        EitherT(MF().raiseError<A>(e).map { it.right() })
    }

    override fun <A, B> Kind<EitherTPartialOf<F, L>, A>.flatMap(f: (A) -> Kind<EitherTPartialOf<F, L>, B>): EitherT<F, L, B> =
        MF().run {
            this@flatMap.fix().flatMap(this) { f(it).fix() }
        }

    override fun <A, B> tailRecM(a: A, f: (A) -> Kind<EitherTPartialOf<F, L>, Either<A, B>>): EitherT<F, L, B> =
        (object : EitherTMonadErrorInstance<F, L> {
            override fun MF(): Monad<F> = MF()
            override fun FF(): Functor<F> = MF()
        }).tailRecM(a, f)

    override fun <A> defer(fa: () -> Kind<EitherTPartialOf<F, L>, A>): EitherT<F, L, A> =
        EitherT(MF().defer { fa().fix().value })
}

@instance(OptionT::class)
interface OptionTMonadDefer<F> : MonadDefer<OptionTPartialOf<F>> {

    fun MF(): MonadDefer<F>

    override fun <A> just(a: A): OptionT<F, A> = OptionT.just(MF(), a)

    override fun <A> Kind<OptionTPartialOf<F>, A>.handleErrorWith(f: (Throwable) -> Kind<OptionTPartialOf<F>, A>): OptionT<F, A> =
        (object : OptionTMonadError<F, Throwable> {
            override fun FF(): MonadError<F, Throwable> = MF()
        }).run {
            handleErrorWith(f)
        }

    override fun <A> raiseError(e: Throwable): OptionT<F, A> =
        (object : OptionTMonadError<F, Throwable> {
            override fun FF(): MonadError<F, Throwable> = MF()
        }).raiseError(e)

    override fun <A, B> Kind<OptionTPartialOf<F>, A>.flatMap(f: (A) -> Kind<OptionTPartialOf<F>, B>): OptionT<F, B> =
        MF().run {
            this@flatMap.fix().flatMap(this) { f(it).fix() }
        }

    override fun <A, B> tailRecM(a: A, f: (A) -> Kind<OptionTPartialOf<F>, Either<A, B>>): OptionT<F, B> =
        (object : OptionTMonadError<F, Throwable> {
            override fun FF(): MonadError<F, Throwable> = MF()
        }).tailRecM(a, f)

    override fun <A> defer(fa: () -> Kind<OptionTPartialOf<F>, A>): OptionT<F, A> =
        OptionT(MF().defer { fa().fix().value })
}

@instance(OptionT::class)
interface OptionTMonadError<F, E> : MonadError<OptionTPartialOf<F>, E>, OptionTMonadInstance<F> {
    override fun FF(): MonadError<F, E>

    override fun <A> raiseError(e: E): OptionT<F, A> = FF().run {
        OptionT(FF().raiseError<A>(e).map { it.some() })
    }

    override fun <A> Kind<OptionTPartialOf<F>, A>.handleErrorWith(f: (E) -> Kind<OptionTPartialOf<F>, A>): OptionT<F, A> =
        FF().run {
            OptionT(this@handleErrorWith.fix().value.handleErrorWith { f(it).fix().value })
        }
}

@instance(StateT::class)
interface StateTMonadDefer<F, S> : MonadDefer<StateTPartialOf<F, S>> {

    fun MF(): MonadDefer<F>

    override fun <A> just(a: A): StateT<F, S, A> = StateT.just(MF(), a)

    override fun <A> Kind<StateTPartialOf<F, S>, A>.handleErrorWith(f: (Throwable) -> Kind<StateTPartialOf<F, S>, A>): StateT<F, S, A> =
        MF().run {
            StateT(MF()) { s ->
                this@handleErrorWith.fix().run(MF(), s).handleErrorWith { e -> f(e).fix().run(MF(), s) }
            }
        }

    override fun <A> raiseError(e: Throwable): StateT<F, S, A> =
        liftFStateT(MF(), MF().raiseError(e))

    override fun <A, B> Kind<StateTPartialOf<F, S>, A>.flatMap(f: (A) -> Kind<StateTPartialOf<F, S>, B>): StateT<F, S, B> =
        MF().run {
            this@flatMap.fix().flatMap(this) { f(it).fix() }
        }

    override fun <A, B> tailRecM(a: A, f: (A) -> Kind<StateTPartialOf<F, S>, Either<A, B>>): StateT<F, S, B> =
        (object : StateTMonadInstance<F, S> {
            override fun FF(): Monad<F> = MF()
        }).tailRecM(a, f)

    override fun <A> defer(fa: () -> Kind<StateTPartialOf<F, S>, A>): StateT<F, S, A> =
        StateT.invokeF(MF().defer { fa().fix().runF })
}

fun <F, S, A> liftFStateT(MF: MonadDefer<F>, fa: Kind<F, A>): StateT<F, S, A> = MF.run {
    StateT(MF) { s -> fa.map { a -> Tuple2(s, a) } }
}

@instance(WriterT::class)
interface WriterTMonadDefer<F, L> : MonadDefer<WriterTPartialOf<F, L>> {

    fun MF(): MonadDefer<F>

    fun ML(): Monoid<L>

    override fun <A> just(a: A): WriterT<F, L, A> =
        WriterT.value(MF(), ML(), a)

    override fun <A> Kind<WriterTPartialOf<F, L>, A>.handleErrorWith(f: (Throwable) -> Kind<WriterTPartialOf<F, L>, A>): WriterT<F, L, A> =
        (object : WriterTMonadError<F, L, Throwable> {
            override fun FF(): MonadError<F, Throwable> = MF()
            override fun MM(): Monoid<L> = ML()
        }).run {
            this@handleErrorWith.fix().handleErrorWith(f)
        }

    override fun <A> raiseError(e: Throwable): WriterT<F, L, A> =
        (object : WriterTMonadError<F, L, Throwable> {
            override fun FF(): MonadError<F, Throwable> = MF()
            override fun MM(): Monoid<L> = ML()
        }).raiseError(e)

    override fun <A, B> Kind<WriterTPartialOf<F, L>, A>.flatMap(f: (A) -> Kind<WriterTPartialOf<F, L>, B>): WriterT<F, L, B> =
        MF().run {
            this@flatMap.fix().flatMap(this, ML()) { f(it).fix() }
        }

    override fun <A, B> tailRecM(a: A, f: (A) -> Kind<WriterTPartialOf<F, L>, Either<A, B>>): WriterT<F, L, B> =
        (object : WriterTMonadInstance<F, L> {
            override fun FF(): MonadError<F, Throwable> = MF()
            override fun MM(): Monoid<L> = ML()
        }).tailRecM(a, f)

    override fun <A> defer(fa: () -> Kind<WriterTPartialOf<F, L>, A>): WriterT<F, L, A> =
        WriterT(MF().defer { fa().fix().value })
}

@instance(WriterT::class)
interface WriterTMonadError<F, L, E> : MonadError<WriterTPartialOf<F, L>, E>, WriterTMonadInstance<F, L>, WriterTApplicativeError<F, L, E> {
}

@instance(WriterT::class)
interface WriterTApplicativeError<F, L, E> : ApplicativeError<WriterTPartialOf<F, L>, E>, WriterTApplicativeInstance<F, L> {

    override fun FF(): MonadError<F, E>

    override fun <A> raiseError(e: E): WriterT<F, L, A> = WriterT(FF().raiseError<Tuple2<L, A>>(e))

    override fun <A> Kind<WriterTPartialOf<F, L>, A>.handleErrorWith(f: (E) -> Kind<WriterTPartialOf<F, L>, A>): WriterT<F, L, A> = FF().run {
        WriterT(this@handleErrorWith.fix().value.handleErrorWith { e -> f(e).fix().value })
    }
}

@instance(Kleisli::class)
interface KleisliMonadDefer<F, R> : MonadDefer<KleisliPartialOf<F, R>> {

    fun MF(): MonadDefer<F>

    override fun <A> Kind<KleisliPartialOf<F, R>, A>.handleErrorWith(f: (Throwable) -> Kind<KleisliPartialOf<F, R>, A>): Kleisli<F, R, A> = MF().run {
        Kleisli { r ->
            MF().defer { this@handleErrorWith.fix().run(r).handleErrorWith { e -> f(e).fix().run(r) } }
        }
    }

    override fun <A, B> Kind<KleisliPartialOf<F, R>, A>.flatMap(f: (A) -> Kind<KleisliPartialOf<F, R>, B>): Kleisli<F, R, B> = MF().run {
        Kleisli { r -> MF().defer { this@flatMap.fix().run(r).flatMap(f.andThen { it.fix().run(r) }) } }
    }

    override fun <A> defer(fa: () -> Kind<KleisliPartialOf<F, R>, A>): Kleisli<F, R, A> =
        Kleisli { r -> MF().defer { fa().fix().run(r) } }
}
