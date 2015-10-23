package springnz.sparkplug.examples

import springnz.sparkplug.core.SparkFactory
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import springnz.sparkplug.core._
import springnz.util.Logging

import better.files._

import scala.util.Try

object LetterCount extends LocalExecutable("LetterCount") {
  def main(args: Array[String]): Unit = {
    s"StartLetterCount: ${DateTime.now()}" >>: (home / "testfile.txt")

    val result: Try[(Long, Long)] = executor.execute((new LetterCount)())

    s"result = ${result.get}" >>: (home / "testfile.txt")
    s"goodbye: ${DateTime.now()}\n" >>: (home / "testfile.txt")
  }
}

class LetterCountFactory extends LetterCount with SparkFactory {
  override def apply(input: Any): SparkOperation[(Long, Long)] = super.apply()
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

