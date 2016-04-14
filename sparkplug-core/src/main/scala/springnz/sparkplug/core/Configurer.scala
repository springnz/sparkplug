package springnz.sparkplug.core

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.SparkConf
import springnz.sparkplug.util.ConfigUtils

trait Configurer {
  def apply[A](f: SparkConf ⇒ A): A = {
    val cfg = new SparkConf()
    f(cfg)
  }
}

trait MapConfigurer extends Configurer {
  def configMap: Map[String, String]

  override def apply[A](f: SparkConf ⇒ A): A = {
    val config = super.apply { cfg ⇒
      configMap.foldLeft(cfg) {
        case (configState, (key, value)) ⇒
          configState.set(key, value.toString)
      }
    }
    f(config)
  }

  override def toString = configMap.filterNot(_._1.contains("password")).mkString("\n")
}

class LocalConfigurer(
    applicationName: String,
    sparkMaster: Option[String] = None,
    configOption: Option[Config] = Some(LocalConfigurer.defaultConfig)) extends MapConfigurer {

  protected def appName = applicationName

  private val configFields = configOption.map(config ⇒ ConfigUtils.configFields(config, "")).getOrElse(Map.empty)

  override def configMap = (sparkMaster match {
    case Some(masterName) ⇒ configFields.updated("spark.master", masterName)
    case None             ⇒ configFields
  }) ++ Map("spark.app.name" -> appName)

}

object LocalConfigurer {
  def defaultConfig = ConfigFactory.load.getConfig("sparkplug.spark.conf")
}
