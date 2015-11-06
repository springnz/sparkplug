package springnz.sparkplug.elasticsearch

import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import springnz.sparkplug.core.SparkOperation

import scala.reflect.ClassTag
import scala.util.Try
import scalaz.syntax.bind._

object ESExporter {
  type ESExportResult[K, V] = (RDD[Map[K, V]], Try[Unit])
}

class ESExporter[K: ClassTag, V: ClassTag](
  resourceIndex: String,
  resourceType: String,
  idField: String = "",
  includeFields: String = "",
  excludeFields: String = "",
  extraConfig: Map[String, String] = Map.empty)
    extends Serializable {

  import ESExporter._

  val configMap: Map[String, String] = Map[String, String](
    "es.mapping.id" -> idField,
    "es.mapping.include" -> includeFields,
    "es.mapping.exclude" -> excludeFields)
    .filter { case (_, v) ⇒ v.nonEmpty } ++ extraConfig

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
