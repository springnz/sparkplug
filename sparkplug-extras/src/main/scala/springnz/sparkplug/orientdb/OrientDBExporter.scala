//package springnz.sparkplug.elasticsearch
//
//import org.apache.spark.rdd.RDD
//import springnz.sparkplug.core.SparkOperation
//import springnz.sparkplug.core.Logging
//
//import scala.util.Try
//
//object OrientDBExporter extends Logging {
//
//  import com.metreta.spark.orientdb.connector._
//  import springnz.sparkplug.core.Pimpers._
//
//  case class OrientExportResult[A](rdd: RDD[A], result: Try[Unit])
//
//  def exportJson(className: String)(rdd: RDD[String]): SparkOperation[OrientExportResult[String]] = SparkOperation { ctx =>
//    val result: Try[Unit] = Try {
//      rdd.saveJsonToOrient(className)
//    }.withErrorLog(s"Error saving to '$className'")
//    OrientExportResult(rdd, result)
//  }
//
//  def export[A](className: String)(rdd: RDD[A]): SparkOperation[OrientExportResult[A]] = SparkOperation { ctx =>
//    val result: Try[Unit] = Try {
//      rdd.saveToOrient(className)
//    }.withErrorLog(s"Error saving to '$className'")
//    OrientExportResult(rdd, result)
//  }
//
//}
//
// TODO: use or remove

