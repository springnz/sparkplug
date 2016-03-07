package springnz.sparkplug.examples

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import springnz.sparkplug.core._

import springnz.sparkplug.util.Logging

import scala.util.Try

object LetterCount extends LocalExecutable("LetterCount") with Logging {
  def main(args: Array[String]): Unit = {
    log.info(s"StartLetterCount: ${DateTime.now()}")
    val result: Try[(Long, Long)] = executor.execute((new LetterCount)())
    log.info(s"Result of LetterCount.main: $result")
  }
}

class LetterCountPlugin extends SparkPlugin[(Long, Long)] {
  val lc = new LetterCount()
  override def apply(): SparkOperation[(Long, Long)] = lc()
}

class LetterCount extends Logging {

  def apply(): SparkOperation[(Long, Long)] = SparkOperation { ctx ⇒

    val textRDDProvider = SparkOperation[RDD[String]] {
      ctx ⇒ ctx.makeRDD("There is nothing either good or bad, but thinking makes it so".split(' '))
    }

    val nums = for {
      // on-site decision what to plug in - different to VaultEmails for example
      logData ← textRDDProvider
      numAs = logData.filter(_.contains("a")).count()
      numBs = logData.filter(_.contains("b")).count()
    } yield {
      log.info(s"$numAs 'a's, $numBs 'b's")
      (numAs, numBs)
    }

    nums.run(ctx)
  }
}

object LetterCountFunctionStyle extends (() ⇒ SparkOperation[(Long, Long)]) with Logging {

  def apply(): SparkOperation[(Long, Long)] = SparkOperation { ctx ⇒
    val textRDDProvider = SparkOperation[RDD[String]] {
      ctx ⇒ ctx.makeRDD("There is nothing either good or bad, but thinking makes it so".split(' '))
    }

    val nums = for {
      // on-site decision what to plug in - different to VaultEmails for example
      logData ← textRDDProvider
      numAs = logData.filter(_.contains("a")).count()
      numBs = logData.filter(_.contains("b")).count()
    } yield {
      log.info(s"$numAs 'a's, $numBs 'b's")
      (numAs, numBs)
    }

    nums.run(ctx)
  }
}

