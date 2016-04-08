package springnz.sparkplug.core

import com.typesafe.config.ConfigFactory
import springnz.sparkplug.util.Logging

import scala.util.{ Failure, Success, Try }

object ConfigEnvironment extends Logging {

  val config = {
    val rootConfig = ConfigFactory.load()
    Try {
      rootConfig.getString("sparkplug-config-root-path")
    } match {
      case Success(path) ⇒
        log.info(s"Setting root config path [$path]")
        rootConfig.getConfig(path)
      case Failure(_) ⇒
        rootConfig
    }
  }
}
