package kollect

import arrow.Kind
import arrow.core.PartialFunction
import arrow.data.ForNonEmptyList
import arrow.data.NonEmptyList
import arrow.data.fix

/**
 * Builds a new [List] by applying a partial function to all the elements from this [NonEmptyList] on which the function
 * is defined.
 */
fun <A, B> Kind<ForNonEmptyList, A>.collect(pf: PartialFunction<A, B>): List<B> =
        if (pf.isDefinedAt(fix().head)) {
            listOf(pf.invoke(fix().head)) + if (fix().tail.isEmpty()) {
                listOf()
            } else {
                NonEmptyList.fromListUnsafe(fix().tail).collect(pf)
            }
        } else {
            if (fix().tail.isEmpty()) listOf() else NonEmptyList.fromListUnsafe(fix().tail).collect(pf)
        }
