package kollect.test

import arrow.Kind
import arrow.core.Option
import arrow.core.Right
import arrow.core.Tuple2
import arrow.data.ListK
import arrow.data.extensions.list.traverse.traverse
import arrow.data.fix
import arrow.effects.IO
import arrow.effects.extensions.concurrent
import arrow.effects.extensions.io.concurrent.concurrent
import arrow.effects.extensions.io.functor.map
import arrow.effects.typeclasses.Concurrent
import io.kotlintest.runner.junit4.KotlinTestRunner
import io.kotlintest.shouldBe
import io.kotlintest.specs.AbstractStringSpec
import kollect.DataSource
import kollect.Kollect
import kollect.extensions.io.timer.timer
import kollect.extensions.kollect.monad.monad
import kollect.fix
import kollect.test.DataSources.Article
import kollect.test.DataSources.Author
import kollect.test.DataSources.article
import kollect.test.DataSources.author
import org.junit.runner.RunWith
import kotlin.coroutines.EmptyCoroutineContext

@RunWith(KotlinTestRunner::class)
class KollectAsyncQueryTests : AbstractStringSpec() {
    init {

        "We can interpret an async kollect into an IO" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Article> = article(CF, 1)

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { it shouldBe Article(1, "An article with id 1") }.unsafeRunSync()
        }

        "We can combine several async data sources and interpret a kollect into an IO" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<Article, Author>> =
                    Kollect.monad<F, Int>(CF).binding {
                        val art = article(CF, 1).bind()
                        val author = author(CF, art).bind()
                        Tuple2(art, author)
                    }.fix()

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map { it shouldBe Tuple2(Article(1, "An article with id 1"), Author(2, "@egg2")) }.unsafeRunSync()
        }

        "We can use combinators in a for comprehension and interpret a kollect from async sources into an IO" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, List<Article>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val articles = listOf(1, 1, 2).traverse(monad) { article(CF, it) }.bind()
                    articles.fix()
                }.fix()
            }

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map {
                it shouldBe listOf(
                        Article(1, "An article with id 1"),
                        Article(1, "An article with id 1"),
                        Article(2, "An article with id 2")
                )
            }.unsafeRunSync()
        }

        "We can use combinators and multiple sources in a for comprehension and interpret a kollect from async sources into an IO" {
            fun <F> kollect(CF: Concurrent<F>): Kollect<F, Tuple2<ListK<Article>, ListK<Author>>> {
                val monad = Kollect.monad<F, Int>(CF)
                return monad.binding {
                    val articles = listOf(1, 1, 2).traverse(monad) { article(CF, it) }.bind().fix()
                    val authors = articles.traverse(monad) { author(CF, it) }.bind().fix()
                    Tuple2(articles, authors)
                }.fix()
            }

            val io = Kollect.run(IO.concurrent(), IO.timer(EmptyCoroutineContext), kollect(IO.concurrent()))
            io.map {
                it shouldBe Tuple2(
                        listOf(
                                Article(1, "An article with id 1"),
                                Article(1, "An article with id 1"),
                                Article(2, "An article with id 2")
                        ),
                        listOf(
                                Author(2, "@egg2"),
                                Author(2, "@egg2"),
                                Author(3, "@egg3")
                        ))
            }.unsafeRunSync()
        }
    }
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
