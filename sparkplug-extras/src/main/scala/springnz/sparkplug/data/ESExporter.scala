package springnz.sparkplug.data

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import springnz.sparkplug.core.{ SparkOperation, SparkProcess }
import springnz.sparkplug.data.ESExporter._

import scala.reflect.ClassTag
import scala.util.Try
import scalaz.syntax.bind._

object ESExporter {
  type ESExportResult[K, V] = (RDD[Map[K, V]], Try[Unit])
}

class ESExporter[K: ClassTag, V: ClassTag](dataQuery: SparkOperation[RDD[Map[K, V]]],
  resourceIndex: String,
  resourceType: String,
  idField: String = "",
  includeFields: String = "",
  excludeFields: String = "",
  extraConfig: Map[String, String] = Map.empty)
    extends SparkProcess[ESExportResult[K, V]] with Serializable {

  val config = ConfigFactory.load()
  val indexName = config.getString(s"spark.es.resourceIndex.$resourceIndex")

  val configMap: Map[String, String] = Map[String, String](
    "es.mapping.id" -> idField,
    "es.mapping.include" -> includeFields,
    "es.mapping.exclude" -> excludeFields)
    .filter { case (_, v) ⇒ v.nonEmpty } ++ extraConfig

  override def apply(): SparkOperation[ESExportResult[K, V]] = {

    val savedData: SparkOperation[Try[Unit]] = {
      for (rdd ← dataQuery) yield {
        Try {
          val rdd2 = rdd
          rdd2.saveToEs(s"$indexName/$resourceType", configMap)
        }
      }
    }

    // return the SparkOperation[Unit] to indicate an effect
    (dataQuery ⊛ savedData).tupled
  }
}
