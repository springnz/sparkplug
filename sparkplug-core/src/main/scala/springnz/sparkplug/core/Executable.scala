package springnz.sparkplug.core

import scala.util.Try

trait Executable {
  def executor: Executor[Try[_]]
}

class LocalExecutable(appName: String, sparkMaster: Option[String] = None) extends Executable {
  override def executor = new SparkExecutor {
    override val configurer: Configurer = new LocalConfigurer(appName, sparkMaster)
  }
}
