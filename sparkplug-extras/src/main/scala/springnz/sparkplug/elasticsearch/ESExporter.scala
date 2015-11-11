package springnz.sparkplug.elasticsearch

import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.elasticsearch.ESExporter.ESExporterParams

import scala.reflect.ClassTag
import scala.util.Try
import scalaz.syntax.bind._

object ESExporter {
  type ESExportResult[K, V] = (RDD[Map[K, V]], Try[Unit])

  case class ESExporterParams(nodes: String = "",
    port: String = "",
    idField: String = "",
    includeFields: String = "",
    excludeFields: String = "",
    extraConfig: Map[String, String] = Map.empty)
}

class ESExporter[K: ClassTag, V: ClassTag](
  resourceIndex: String,
  resourceType: String,
  exporterParams: ESExporterParams = ESExporterParams())
    extends Serializable {

  import ESExporter._

  val configMap: Map[String, String] = Map[String, String](
    "es.mapping.id" -> exporterParams.idField,
    "es.nodes" -> exporterParams.nodes,
    "es.port" -> exporterParams.port,
    "es.mapping.include" -> exporterParams.includeFields,
    "es.mapping.exclude" -> exporterParams.excludeFields)
    .filter { case (_, v) ⇒ v.nonEmpty } ++ exporterParams.extraConfig

  def export(dataSource: RDD[Map[K, V]]): SparkOperation[ESExportResult[K, V]] = {
    val savedData: SparkOperation[Try[Unit]] = SparkOperation { _ ⇒
      Try {
        dataSource.saveToEs(s"$resourceIndex/$resourceType", configMap)
      }
    }
    val dataSourceSparkOperation = SparkOperation { _ ⇒ dataSource }

    // return the SparkOperation[Unit] to indicate an effect
    (dataSourceSparkOperation ⊛ savedData).tupled
  }
}

