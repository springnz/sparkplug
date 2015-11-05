package springnz.sparkplug.elasticsearch

import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import springnz.sparkplug.core._
import springnz.sparkplug.data.SparkDataSource
import springnz.util.Logging

class ESJsonRDDSource(resourceIndex: String, resourceType: String)
    extends SparkDataSource[(String, String)] with Logging {

  override def apply(): SparkOperation[RDD[(String, String)]] = SparkOperation { ctx ⇒
    val rdd: RDD[(String, String)] = ctx.esJsonRDD(s"$resourceIndex/$resourceType")
    rdd
  }
}
