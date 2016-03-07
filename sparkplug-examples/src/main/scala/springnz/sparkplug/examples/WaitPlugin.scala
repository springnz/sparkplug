package springnz.sparkplug.examples

import springnz.sparkplug.core.{ SparkOperation, SparkPlugin }
import springnz.sparkplug.util.Logging
import scala.concurrent._

class WaitPlugin(timeout: Long = 2000L) extends SparkPlugin[Unit] with Logging {
  def apply(): SparkOperation[Unit] = SparkOperation { _ ⇒
    blocking { Thread.sleep(timeout) }
  }
}
