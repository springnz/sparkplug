package springnz.sparkplug.core

import org.apache.spark.rdd.RDD
import springnz.sparkplug.util.Logging

import scala.reflect.ClassTag

object SparkOperationPimpers extends Logging {
  class SparkOperationExtensions[A: ClassTag](operation: SparkOperation[RDD[A]]) {
    def mapRDD[B: ClassTag](f: A ⇒ B) = operation.map {
      rdd ⇒ rdd map f
    }

    def filterRDD(f: A ⇒ Boolean) = operation.map {
      rdd ⇒ rdd filter f
    }

    def count = {
      operation.map { rdd ⇒
        val n = rdd.count
        log.debug(s"Produced an RDD of size ${n}")
        n
      }
    }

    def collect(): SparkOperation[Array[A]] = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        rdd.collect()
      }
    }

    def take(n: Int): SparkOperation[Array[A]] = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        rdd.take(n)
      }
    }

    def takeOrdered(n: Int)(implicit ord: Ordering[A]): SparkOperation[Array[A]] = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        rdd.takeOrdered(n)
      }
    }

    def collectWithCount(): SparkOperation[(Array[A], Long)] = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        (rdd.collect, count)
      }
    }

    def takeWithCount(n: Int): SparkOperation[(Array[A], Long)] = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        (rdd.take(n), count)
      }
    }

    def takeOrderedWithCount(n: Int)(implicit ord: Ordering[A]): SparkOperation[(Array[A], Long)] = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        (rdd.takeOrdered(n), count)
      }
    }
  }
}
