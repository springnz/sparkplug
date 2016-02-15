package springnz.sparkplug.testkit

import springnz.sparkplug.core._

class SimpleTestContext(appName: String) extends SparkExecutor {
  override protected def configurer: Configurer = new LocalConfigurer(appName)
}
