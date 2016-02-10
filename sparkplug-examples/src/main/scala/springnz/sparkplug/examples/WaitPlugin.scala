package springnz.sparkplug.examples

import springnz.sparkplug.core.{ SparkOperation, SparkPlugin }
import springnz.util.Logging
import scala.concurrent._
import scala.reflect.ClassTag

class WaitPlugin extends SparkPlugin with Logging {

  override type A = Nothing
  override type R = Unit

  override def apply(input: Option[Nothing]): SparkOperation[Unit] = SparkOperation { _ ⇒
    val waitMillis = input.getOrElse(2000).toString.toLong
    blocking { Thread.sleep(waitMillis) }
  }
}
