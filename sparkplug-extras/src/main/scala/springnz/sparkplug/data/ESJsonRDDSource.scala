package springnz.sparkplug.data

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import springnz.sparkplug.core._
import springnz.util.Logging

class ESJsonRDDSource(resourceIndex: String, resourceType: String)
    extends SparkDataSource[(String, String)] with Logging {

  val config = ConfigFactory.load()
  val indexName = config.getString(s"spark.es.resourceIndex.$resourceIndex")

  override def apply(): SparkOperation[RDD[(String, String)]] = SparkOperation { ctx ⇒
    val rdd: RDD[(String, String)] = ctx.esJsonRDD(s"$indexName/$resourceType")
    rdd
  }
}
