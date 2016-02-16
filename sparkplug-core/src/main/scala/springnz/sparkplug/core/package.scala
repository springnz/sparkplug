package springnz.sparkplug

import org.apache.spark.rdd.RDD
import springnz.sparkplug.core.SparkOperationPimpers._
import springnz.sparkplug.core.RDDPimpers._

import scala.language.implicitConversions
import scala.reflect.ClassTag

package object core {

  implicit def toRDDExtensions[A: ClassTag](rdd: RDD[A]) = new RDDExtensions[A](rdd)

  implicit def toSparkOperationExtensions[A: ClassTag](operation: SparkOperation[RDD[A]]) = new SparkOperationExtensions(operation)
}
