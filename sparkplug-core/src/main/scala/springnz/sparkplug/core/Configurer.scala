package springnz.sparkplug.core

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf

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

class LocalConfigurer(applicationName: String, sparkMaster: Option[String] = None) extends MapConfigurer {
  import scala.collection.JavaConversions._

  protected val appConfig = ConfigFactory.load()

  protected def appName = applicationName

  protected val configFields: Map[String, String] = {
    val sparkConfig = appConfig.getConfig("spark.conf")
    sparkConfig.entrySet
      .map {
        case entry ⇒
          val key = entry.getKey
          (key, sparkConfig.getString(key))
      }
      .toMap
  }

  override def configMap = (sparkMaster match {
    case Some(masterName) ⇒ configFields.updated("spark.master", masterName)
    case None             ⇒ configFields
  }) ++ Map("spark.app.name" -> appName)

}
