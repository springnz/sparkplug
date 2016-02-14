package springnz.sparkplug

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.testkit.TestExtensions.{ SparkDFOps, SparkRDDOps }

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by stephen on 15/02/16.
  */
package object testkit {
  implicit def testOps[A: ClassTag](operation: SparkOperation[RDD[A]]) = new SparkRDDOps(operation)

  implicit def testOps[A: ClassTag](operation: SparkOperation[DataFrame]) = new SparkDFOps(operation)
}
