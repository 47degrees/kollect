package kollect.arrow.instances

import arrow.instance
import arrow.typeclasses.Monoid
import arrow.typeclasses.Semigroup
import kollect.InMemoryCache

@instance(InMemoryCache::class)
interface InMemoryCacheMonoidInstance : Semigroup<InMemoryCache>, Monoid<InMemoryCache> {

    override fun empty(): InMemoryCache = InMemoryCache.empty()

    override fun InMemoryCache.combine(b: InMemoryCache): InMemoryCache =
        InMemoryCache(this.state.plus(b.state))
}
