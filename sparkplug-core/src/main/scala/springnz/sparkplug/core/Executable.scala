package springnz.sparkplug.core

import com.typesafe.config.Config

import scala.util.Try

trait Executable {
  def executor: Executor[Try[_]]
}

class LocalExecutable(
    appName: String,
    sparkMaster: Option[String] = None,
    config: Option[Config] = Some(LocalConfigurer.defaultConfig)) extends Executable {

  override def executor = new SparkExecutor {
    override val configurer: Configurer = new LocalConfigurer(appName, sparkMaster)
  }
}
