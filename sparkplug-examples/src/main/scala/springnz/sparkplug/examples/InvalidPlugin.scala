package springnz.sparkplug.examples

import springnz.sparkplug.core.{ SparkOperation, SparkPlugin }

class InvalidPlugin extends SparkPlugin[Unit] {
  override def apply(): SparkOperation[Unit] = throw new Exception("whoops")
}
