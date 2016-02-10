package springnz.sparkplug.client

import springnz.sparkplug.examples.LetterCountPlugin

import scala.concurrent._
import scala.concurrent.duration._

object SparkPlugClient {
  def main(args: Array[String]): Unit = {
    val executor = ClientExecutor.create()

    val futures: List[Future[(Long, Long)]] = List.fill(10) {
      executor.execute(() ⇒ new LetterCountPlugin(), None)
    }

    implicit val ec = scala.concurrent.ExecutionContext.global
    val sequence: Future[List[(Long, Long)]] = Future.sequence(futures)
    Await.result(sequence, 120.seconds)
    executor.shutDown()
  }
}

