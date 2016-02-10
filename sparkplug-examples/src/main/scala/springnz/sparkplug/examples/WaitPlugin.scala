package springnz.sparkplug.examples

import springnz.sparkplug.core.{ DescriptorAux, SparkOperation, SparkPlugin }
import springnz.util.Logging
import scala.concurrent._
import scala.reflect.ClassTag

trait WaitPluginDescriptor extends DescriptorAux[Nothing, Unit] {
  override val classname: String = "springnz.sparkplug.examples.WaitPlugin"
}

object WaitPluginDescriptor extends WaitPluginDescriptor

class WaitPlugin extends SparkPlugin with WaitPluginDescriptor with Logging {
  override def apply(input: Option[Nothing]): SparkOperation[Unit] = SparkOperation { _ ⇒
    val waitMillis = input.getOrElse(2000).toString.toLong
    blocking { Thread.sleep(waitMillis) }
  }
}
