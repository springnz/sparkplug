package springnz.sparkplug.core

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

// TODO: test these classes
object CoreExtensions {
  implicit class SparkRDDOps[A: ClassTag](operation: SparkOperation[RDD[A]]) {
    def mapRDD[B: ClassTag](f: A ⇒ B) = operation.map {
      rdd ⇒ rdd map f
    }

    def filterRDD(f: A ⇒ Boolean) = operation.map {
      rdd ⇒ rdd filter f
    }

  }
}
