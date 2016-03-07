import springnz.sparkplug.client.ClientExecutor
import springnz.sparkplug.examples.LetterCountPlugin

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

object Main {
  def main(args: Array[String]): Unit = {

    val executor = ClientExecutor.create()

    val futures: List[Future[(Long, Long)]] = List.fill(10) {
      executor.execute(() ⇒ new LetterCountPlugin())
    }

    implicit val ec = scala.concurrent.ExecutionContext.global
    val sequence: Future[List[(Long, Long)]] = Future.sequence(futures)
    Await.result(sequence, 20.seconds)
    executor.shutDown()
  }
}
