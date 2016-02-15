package springnz.sparkplug.elasticsearch

import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import springnz.sparkplug.core._
import springnz.sparkplug.util.Logging

object ESJsonRDDSource
    extends Logging {

  case class ESDataSourceParams(nodes: String = "",
    port: Option[Int] = None,
    query: String = "",
    extraConfig: Map[String, String] = Map.empty)

  def apply(resourceIndex: String, resourceType: String, dataSourceParams: ESDataSourceParams = ESDataSourceParams()): SparkOperation[RDD[(String, String)]] = SparkOperation { ctx ⇒

    val configMap: Map[String, String] = Map[String, String](
      "es.nodes" -> dataSourceParams.nodes,
      "es.port" -> dataSourceParams.port.optionToString(),
      "es.query" -> dataSourceParams.query)
      .filter { case (_, v) ⇒ v.nonEmpty } ++ dataSourceParams.extraConfig

    log.info(s"Fetching data from elasticsearch $resourceIndex/$resourceType with config: $configMap")

    val rdd: RDD[(String, String)] = ctx.esJsonRDD(s"$resourceIndex/$resourceType", configMap)
    rdd
  }

  // TODO: ? move this elsewhere
  implicit class OptionToString[A](input: Option[A]) {
    def optionToString(): String = input match {
      case Some(inner) ⇒ inner.toString
      case None        ⇒ ""
    }
  }
}
