package springnz.sparkplug.core

import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object SparkPimpers {

  class RDDPimperOps[A: ClassTag](rdd: RDD[A]) {
    def mapPartial[B: ClassTag](defaultValue: ⇒ B)(f: PartialFunction[A, B]): RDD[B] = {
      rdd.map {
        a ⇒ if (f.isDefinedAt(a)) f(a) else defaultValue
      }
    }

    def mapWithFilter[B: ClassTag](f: A ⇒ Option[B]): RDD[B] = {
      rdd.map(f).filter(_.isDefined)
        .map(_.get)
    }

    def mapPartialWithFilter[B: ClassTag](f: PartialFunction[A, Option[B]]): RDD[B] = {
      rdd.map {
        a ⇒ if (f.isDefinedAt(a)) f(a) else None
      }.filter(_.isDefined)
        .map(_.get)
    }
  }

  // Converts RDDs to Types that can be verified in test assertions
  class RDDResultConverterOps[A: ClassTag](operation: SparkOperation[RDD[A]])(implicit log: Logger) {
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

