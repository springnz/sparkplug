package springnz.sparkplug.orientdb

import com.metreta.spark.orientdb.connector._
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.spark.rdd.RDD
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.util.Logging

import scala.reflect.ClassTag

object OrientRDDFetcher extends Logging {

  def select[A: ClassTag](
    className: String, where: Option[String], docTransform: ODocument ⇒ A): SparkOperation[RDD[A]] =
    SparkOperation { ctx ⇒
      val result = where match {
        case Some(where) ⇒
          log.info(s"Executing query: select * from [$className] where [$where]")
          ctx.orientDocumentQuery(className, where)
        case None ⇒
          log.info(s"Executing query: select * from [$className]")
          ctx.orientDocumentQuery(className)
      }
      result.map(docTransform)
    }

}
