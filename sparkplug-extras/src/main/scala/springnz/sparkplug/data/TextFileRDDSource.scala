package springnz.sparkplug.data

import org.apache.spark.rdd.RDD
import springnz.sparkplug.core.SparkOperation

object TextFileRDDSource {
  def apply(filename: String, partitions: Int): SparkOperation[RDD[String]] = SparkOperation { ctx ⇒ ctx.textFile(filename, partitions) }
}
