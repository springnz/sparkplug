package springnz.sparkplug.util

import com.typesafe.config.Config

import scala.collection.JavaConversions._

object ConfigUtils {

  def configFields(appConfig: Config, fieldBase: String): Map[String, String] = {
    val sparkConfig =
      if (fieldBase.isEmpty || !appConfig.hasPath(fieldBase)) appConfig else appConfig.getConfig(fieldBase)

    sparkConfig.entrySet
      .map {
        case entry ⇒
          val key = entry.getKey
          (key, sparkConfig.getString(key))
      }
      .toMap
  }

}
