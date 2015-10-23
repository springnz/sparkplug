package springnz.sparkplug.core

import org.apache.spark.rdd.RDD
import springnz.util.Logging

object SparkConverters extends Logging {

  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
    (new java.util.Properties /: map) { case (props, (k, v)) ⇒ props.put(k, v); props }
  }

  // Makes a count method available on a SparkOperation that returns a RDD
  implicit class CountConverter[A](operation: SparkOperation[RDD[A]]) {
    def count = {
      operation.map { rdd ⇒
        val n = rdd.count
        log.debug(s"Produced an RDD of size ${n}")
        n
      }
    }

    def takeWithCount(n: Int) = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        (rdd.take(n), count)
      }
    }

    def collectWithCount() = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        (rdd.collect, count)
      }
    }

    def takeOrderedWithCount(n: Int)(implicit ord: Ordering[A]) = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        (rdd.takeOrdered(n), count)
      }
    }

  }
}

