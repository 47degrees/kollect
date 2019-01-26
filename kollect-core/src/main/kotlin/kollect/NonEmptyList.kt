package kollect

import arrow.Kind
import arrow.core.PartialFunction
import arrow.data.ForNonEmptyList
import arrow.data.NonEmptyList
import arrow.data.fix
import arrow.data.nel

/**
 * Builds a new `List` by applying a partial function to
 * all the elements from this `NonEmptyList` on which the function is defined
 *
 * {{{
 * scala> import cats.data.NonEmptyList
 * scala> val nel = NonEmptyList.of(1, 2, 3, 4, 5)
 * scala> nel.collect { case v if v < 3 => v }
 * res0: scala.collection.immutable.List[Int] = List(1, 2)
 * scala> nel.collect {
 *      |  case v if v % 2 == 0 => "even"
 *      |  case _ => "odd"
 *      | }
 * res1: scala.collection.immutable.List[String] = List(odd, even, odd, even, odd)
 * }}}
 */
fun <A, B> Kind<ForNonEmptyList, A>.collect(pf: PartialFunction<A, B>): List<B> = if (pf.isDefinedAt(fix().head)) {
    NonEmptyList(pf.invoke(fix().head), fix().tail.nel().collect<A, B>(pf))
} else {
    NonEmptyList.fromListUnsafe(fix().tail.nel().collect<A, B>(pf))
}.all
