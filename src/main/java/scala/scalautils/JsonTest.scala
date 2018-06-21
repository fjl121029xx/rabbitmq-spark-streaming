package scala.scalautils

//隐式转换必须要导入
import org.json4s._
import org.json4s.jackson.JsonMethods._


class Book(val author: String, val content: String, val id: String, val time: Long, val title: String) {
  override def toString: String = {
    author
  }
}

object JsonTest {
  def main(args: Array[String]) {
    val json = "{\"author\":\"hll\",\"content\":\"ES\",\"id\":\"693\",\"time\":1490165237200,\"title\":\"\"}"

    implicit val formats = DefaultFormats

    val book: Book = parse(json).extract[Book]
    println(book)
  }
}
