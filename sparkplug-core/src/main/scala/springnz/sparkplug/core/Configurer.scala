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
    config: Config = ConfigFactory.load()) extends MapConfigurer {

  protected def appName = applicationName

  private val configFields = ConfigUtils.configFields(config, "spark.conf")

  override def configMap = (sparkMaster match {
    case Some(masterName) ⇒ configFields.updated("spark.master", masterName)
    case None             ⇒ configFields
  }) ++ Map("spark.app.name" -> appName)

}
