package kollect

import arrow.Kind
import arrow.data.ForNonEmptyList
import arrow.data.NonEmptyList
import arrow.data.fix

/**
 * Builds a new [List] by applying a partial function to all the elements from this [NonEmptyList] on which the function
 * is defined.
 */
fun <A, B> Kind<ForNonEmptyList, A>.collect(f: (A) -> B, filter: (A) -> Boolean): List<B> =
        if (filter(fix().head)) {
            listOf(f.invoke(fix().head)) + if (fix().tail.isEmpty()) {
                listOf()
            } else {
                NonEmptyList.fromListUnsafe(fix().tail).collect(f, filter)
            }
        } else {
            if (fix().tail.isEmpty()) listOf() else NonEmptyList.fromListUnsafe(fix().tail).collect(f, filter)
        }
