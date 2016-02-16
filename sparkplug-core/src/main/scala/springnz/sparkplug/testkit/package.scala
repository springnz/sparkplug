package springnz.sparkplug

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.testkit.SparkOperationTestPimpers.{ DataFrameExtensions, RDDExtensions }

import scala.language.implicitConversions
import scala.reflect.ClassTag

package object testkit {
  implicit def testOps[A: ClassTag](operation: SparkOperation[RDD[A]]) = new RDDExtensions(operation)

  implicit def testOps[A: ClassTag](operation: SparkOperation[DataFrame]) = new DataFrameExtensions(operation)
}
