package springnz.sparkplug.core

import com.typesafe.config.ConfigFactory
import springnz.sparkplug.util.Logging

import scala.util.Try

object ConfigEnvironment extends Logging {

  val config = {
    val rootConfig = ConfigFactory.load()
    val sharedPrefix: Option[String] = Try { rootConfig.getString("sparkplug-config-shared-prefix") }.toOption
    val environmentPrefix: Option[String] = Try { rootConfig.getString("sparkplug-config-environment-prefix") }.toOption

    sharedPrefix.foreach { prefix ⇒
      log.info(s"Using shared config prefix [$prefix]")
    }

    environmentPrefix.foreach { prefix ⇒
      log.info(s"Using environment config prefix [$prefix]")
    }

    val optionalSharedConfig = sharedPrefix.map(rootConfig.getConfig)
    val optionalEnvironmentConfig = environmentPrefix.map(rootConfig.getConfig)

    (optionalEnvironmentConfig, optionalSharedConfig) match {
      case (Some(environmentConfig), Some(sharedConfig)) ⇒
        environmentConfig.withFallback(sharedConfig)
      case (Some(environmentConfig), None) ⇒
        environmentConfig
      case (None, Some(sharedConfig)) ⇒
        sharedConfig
      case _ ⇒
        log.info("Using root config")
        ConfigFactory.load()
    }
  }
}
