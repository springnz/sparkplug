package springnz.sparkplug.data

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import springnz.sparkplug.core._

trait SparkRDDProcess[A] extends SparkProcess[RDD[A]]
trait SparkDataSource[A] extends SparkRDDProcess[A]
trait SparkIndexedDataSource[A] extends SparkDataSource[(String, A)]
trait SparkDFProcess extends SparkProcess[DataFrame]

