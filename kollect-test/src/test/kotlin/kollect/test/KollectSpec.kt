package kollect.test

import io.kotlintest.specs.AbstractStringSpec
import kollect.KollectQuery
import kollect.Request
import kollect.Round

open class KollectSpec : AbstractStringSpec() {

    fun countFetches(r: Request): Int =
            when (val req = r.request) {
                is KollectQuery.KollectOne<*, *> -> 1
                is KollectQuery.Batch<*, *> -> req.ids.size
            }

    fun totalKollected(rs: List<Round>): Int =
            rs.map { round: Round -> round.queries.map { countFetches(it) }.sum() }.toList().sum()

    fun countBatches(r: Request): Int =
            when (r.request) {
                is KollectQuery.KollectOne<*, *> -> 0
                is KollectQuery.Batch<*, *> -> 1
            }

    fun totalBatches(rs: List<Round>): Int =
            rs.map { round: Round -> round.queries.map { countBatches(it) }.sum() }.toList().sum()
}
