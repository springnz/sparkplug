package springnz.sparkplug.core

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object SparkPimpers {

  implicit class RDDPimper[A: ClassTag](rdd: RDD[A]) {
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

  implicit class RDDOption[A: ClassTag](rdd: RDD[Option[A]]) {

  }
}

