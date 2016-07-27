package springnz.sparkplug.orientdb

import com.metreta.spark.orientdb.connector._
import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.spark.rdd.RDD
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.util.Logging

import scala.reflect.ClassTag

object OrientRDDFetcher extends Logging {

  def select[A: ClassTag](
    className: String,
    whatOption: Option[String],
    whereOption: Option[String],
    docTransform: ODocument ⇒ A,
    orientDBConnector: Option[OrientDBConnector] = None): SparkOperation[RDD[A]] =
    SparkOperation { ctx ⇒
      orientDBConnector.foreach { conn ⇒
        log.info(s"Connecting to OrientDb with protocol '${conn.connProtocol}' and  remote connection string: '${conn.connStringRemote}'")
      }
      val what = whatOption.getOrElse("*")
      val result = whereOption match {
        case Some(where) ⇒
          log.info(s"Executing query: SELECT $what FROM $className WHERE $where")
          orientDBConnector match {
            case Some(connector) ⇒
              ctx.orientDocumentQuery(className, what, where)(connector)
            case None ⇒
              ctx.orientDocumentQuery(className, what, where)
          }
        case None ⇒
          log.info(s"Executing query: SELECT $what FROM $className")
          orientDBConnector match {
            case Some(connector) ⇒
              ctx.orientDocumentQuery(className, what)(connector)
            case None ⇒
              ctx.orientDocumentQuery(className, what)
          }
      }
      result.map(docTransform)
    }

}
