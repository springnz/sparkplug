package springnz.sparkplug

import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD
import springnz.sparkplug.core.CoreExtensionOps._
import springnz.sparkplug.core.SparkPimpers._

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by stephen on 15/02/16.
  */
package object core {

  implicit def rddPimper[A: ClassTag](rdd: RDD[A]) = new RDDPimperOps[A](rdd)

  implicit def resultConverter[A: ClassTag](operation: SparkOperation[RDD[A]]) =
    new RDDResultConverterOps(operation: SparkOperation[RDD[A]])

  implicit def operationPimper[A: ClassTag](operation: SparkOperation[RDD[A]]) = new SparkRDDOps(operation)
}
