package arrow.effects

import arrow.Kind
import arrow.core.Either
import arrow.instance
import arrow.typeclasses.Applicative
import kollect.ForQuery
import kollect.KollectMonadError
import kollect.Query
import kollect.fix
import arrow.effects.handleErrorWith as ioHandleErrorWith

@instance(Query::class)
interface QueryApplicativeInstance : Applicative<ForQuery> {
    override fun <A> just(a: A): Query<A> = Query.just(a)

    override fun <A, B> Kind<ForQuery, A>.ap(ff: Kind<ForQuery, (A) -> B>): Query<B> =
        fix().ap(ff)
}

@instance(IO::class)
interface IOKollectMonadErrorInstance : KollectMonadError<ForIO> {

    override fun <A> just(a: A): IO<A> =
        IO.just(a)

    override fun <A, B> Kind<ForIO, A>.flatMap(f: (A) -> Kind<ForIO, B>): IO<B> =
        fix().flatMap(f)

    override fun <A, B> Kind<ForIO, A>.map(f: (A) -> B): IO<B> =
        fix().map(f)

    override fun <A, B> tailRecM(a: A, f: kotlin.Function1<A, IOOf<arrow.core.Either<A, B>>>): IO<B> =
        IO.tailRecM(a, f)

    override fun <A> Kind<ForIO, A>.attempt(): IO<Either<Throwable, A>> =
        fix().attempt()

    override fun <A> Kind<ForIO, A>.handleErrorWith(f: (Throwable) -> Kind<ForIO, A>): IO<A> =
        fix().ioHandleErrorWith(f)

    override fun <A> raiseError(e: Throwable): IO<A> =
        IO.raiseError(e)

    override fun <A> runQuery(q: Query<A>): Kind<ForIO, A> = TODO()
}
