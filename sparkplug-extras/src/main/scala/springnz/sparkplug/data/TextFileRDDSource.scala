package springnz.sparkplug.data

import org.apache.spark.rdd.RDD
import springnz.sparkplug.core.SparkOperation

class TextFileRDDSource(filename: String, partitions: Int) extends SparkDataSource[String] {
  override def apply(): SparkOperation[RDD[String]] = SparkOperation { ctx ⇒ ctx.textFile(filename, partitions) }
}
