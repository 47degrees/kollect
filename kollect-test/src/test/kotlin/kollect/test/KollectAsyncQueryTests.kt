package kollect.test

import arrow.Kind
import arrow.core.Option
import arrow.core.Right
import arrow.core.Tuple2
import arrow.effects.ForIO
import arrow.effects.IO
import arrow.effects.extensions.io.concurrent.concurrent
import arrow.effects.fix
import arrow.effects.typeclasses.Concurrent
import io.kotlintest.runner.junit4.KotlinTestRunner
import io.kotlintest.shouldBe
import io.kotlintest.specs.AbstractStringSpec
import kollect.DataSource
import kollect.Kollect
import kollect.extensions.io.timer.timer
import kollect.extensions.kollect.monad.flatMap
import kollect.extensions.kollect.monad.map
import kollect.test.DataSources.Article
import kollect.test.DataSources.Author
import kollect.test.DataSources.article
import kollect.test.DataSources.author
import org.junit.runner.RunWith
import kotlin.coroutines.EmptyCoroutineContext

@RunWith(KotlinTestRunner::class)
class KollectAsyncQueryTests : AbstractStringSpec() {
    init {

        "We can interpret an async fetch into an IO" {
            fun kollect(): Kollect<ForIO, Article> = article(IO.concurrent(), 1)

            val io = Kollect.run<ForIO>()(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect())
            val res = io.fix().unsafeRunSync()
            res shouldBe Article(1, "An article with id 1")
        }

        "We can combine several async data sources and interpret a fetch into an IO" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Article, Author>> =
                    article(CF, 1).flatMap<F, String, Article, Tuple2<Article, Author>>(CF) { art ->
                        author(CF, art).map<F, String, Author, Tuple2<Article, Author>>(CF) { author ->
                            Tuple2(art, author)
                        }
                    }

//            CF.binding {
//                val art = article(CF, 1).bind()
//                val author = author(CF, art).bind()
//                Tuple2(art, author)
//            }

            val io = Kollect.run<ForIO>()(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            val res = io.fix().unsafeRunSync()

            res shouldBe Tuple2(Article(1, "An article with id 1"), Author(2, "@egg2"))
        }
    }
//
//    "We can use combinators in a for comprehension and interpret a fetch from async sources into an IO" in {
//        def fetch[F[_] : ConcurrentEffect]: Fetch[F, List[Article]] = for {
//        articles <- List(1, 1, 2).traverse(article[F])
//    } yield articles
//
//        val io = Fetch.run[IO](fetch)
//
//        io.map(_ shouldEqual List(
//                Article(1, "An article with id 1"),
//                Article(1, "An article with id 1"),
//                Article(2, "An article with id 2")
//        )).unsafeToFuture
//    }
//
//    "We can use combinators and multiple sources in a for comprehension and interpret a fetch from async sources into an IO" in {
//        def fetch[F[_] : ConcurrentEffect] = for {
//        articles <- List(1, 1, 2).traverse(article[F])
//        authors  <- articles.traverse(author[F])
//    } yield (articles, authors)
//
//        val io = Fetch.run[IO](fetch)
//
//        io.map(_ shouldEqual (
//                List(
//                        Article(1, "An article with id 1"),
//                        Article(1, "An article with id 1"),
//                        Article(2, "An article with id 2")
//                ),
//                List(
//                        Author(2, "@egg2"),
//                        Author(2, "@egg2"),
//                        Author(3, "@egg3")
//                )
//        )).unsafeToFuture
//    }
}

object DataSources {
    data class ArticleId(val id: Int)
    data class Article(val id: Int, val content: String) {
        fun author(): Int = id + 1
    }

    object ArticleAsync : DataSource<ArticleId, Article> {
        override fun name() = "ArticleAsync"

        override fun <F> fetch(CF: Concurrent<F>, id: ArticleId): Kind<F, Option<Article>> {
            return CF.async { cb ->
                cb(Right(Option(Article(id.id, "An article with id " + id.id))))
            }
        }
    }

    fun <F> article(CF: Concurrent<F>, id: Int): Kollect<F, Article> =
            Kollect(CF, ArticleId(id), ArticleAsync)

    data class AuthorId(val id: Int)
    data class Author(val id: Int, val name: String)

    object AuthorAsync : DataSource<AuthorId, Author> {
        override fun name() = "AuthorAsync"

        override fun <F> fetch(CF: Concurrent<F>, id: AuthorId): Kind<F, Option<Author>> =
                CF.async { cb ->
                    cb(Right(Option(Author(id.id, "@egg" + id.id))))
                }
    }

    fun <F> author(CF: Concurrent<F>, a: Article): Kollect<F, Author> =
            Kollect(CF, AuthorId(a.author()), AuthorAsync)
}
