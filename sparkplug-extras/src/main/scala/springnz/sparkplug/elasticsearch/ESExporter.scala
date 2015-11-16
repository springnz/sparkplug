package springnz.sparkplug.elasticsearch

import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import springnz.sparkplug.core.SparkOperation

import scala.reflect.ClassTag
import scala.util.Try
import scalaz.syntax.bind._

object ESExporter {

  case class ESExportDetails(index: String, configMap: Map[String, String])

  type ESExportResult[K, V] = (RDD[Map[K, V]], Try[ESExportDetails])

  case class ESExporterParams(nodes: String = "",
    port: Option[Int] = None,
    idField: String = "",
    includeFields: String = "",
    excludeFields: String = "",
    extraConfig: Map[String, String] = Map.empty)

  def apply[K: ClassTag, V: ClassTag](resourceIndex: String,
    resourceType: String,
    exporterParams: ESExporterParams = ESExporterParams())(dataSource: RDD[Map[K, V]]): SparkOperation[ESExportResult[K, V]] = {

    val savedData: SparkOperation[Try[ESExportDetails]] = SparkOperation { _ ⇒
      Try {
        import ESJsonRDDSource._
        val portString: String = exporterParams.port.optionToString()
        val configMap: Map[String, String] = Map[String, String](
          "es.mapping.id" -> exporterParams.idField,
          "es.nodes" -> exporterParams.nodes,
          "es.port" -> portString,
          "es.mapping.include" -> exporterParams.includeFields,
          "es.mapping.exclude" -> exporterParams.excludeFields)
          .filter { case (_, v) ⇒ v.nonEmpty } ++ exporterParams.extraConfig

        val details = ESExportDetails(index = s"$resourceIndex/$resourceType", configMap = configMap)
        dataSource.saveToEs(details.index, details.configMap)
        details
      }
    }
    val dataSourceSparkOperation = SparkOperation { _ ⇒ dataSource }

    // return the SparkOperation[Unit] to indicate an effect
    (dataSourceSparkOperation ⊛ savedData).tupled
  }
}

