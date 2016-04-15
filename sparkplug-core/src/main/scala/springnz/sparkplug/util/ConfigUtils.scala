package springnz.sparkplug.util

import com.typesafe.config.Config

import scala.collection.JavaConversions._

object ConfigUtils {

  def configFields(appConfig: Config): Map[String, String] = {
    appConfig.entrySet
      .map {
        case entry ⇒
          val key = entry.getKey
          (key, appConfig.getString(key))
      }
      .toMap
  }

}
