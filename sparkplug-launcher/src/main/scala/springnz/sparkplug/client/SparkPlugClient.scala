package springnz.sparkplug.client

import scala.concurrent._
import scala.concurrent.duration._

object SparkPlugClient {

  def main(args: Array[String]): Unit = {
    val executor = ClientExecutor.create()

    val futures: List[Future[Any]] = List.fill(10) { executor.execute[Any]("springnz.sparkplug.examples.LetterCountPlugin", None) }

    implicit val ec = scala.concurrent.ExecutionContext.global
    val sequence: Future[List[Any]] = Future.sequence(futures)
    Await.result(sequence, 120 seconds)
    executor.shutDown()
  }
}

