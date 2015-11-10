package springnz.sparkplug.elasticsearch

import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import springnz.sparkplug.core._
import springnz.util.Logging

object ESJsonRDDSource
    extends Logging {

  def apply(resourceIndex: String, resourceType: String): SparkOperation[RDD[(String, String)]] = SparkOperation { ctx ⇒
    val rdd: RDD[(String, String)] = ctx.esJsonRDD(s"$resourceIndex/$resourceType")
    rdd
  }
}
