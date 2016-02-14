package springnz.sparkplug.examples

import springnz.sparkplug.core.{ SparkOperation, SparkPlugin }
import springnz.sparkplug.util.Logging
import scala.concurrent._

class WaitPlugin extends SparkPlugin with Logging {
  override def apply(input: Option[Any]): SparkOperation[Unit] = SparkOperation { _ ⇒
    val waitMillis = input.getOrElse(2000).toString.toLong
    blocking { Thread.sleep(waitMillis) }
  }
}
