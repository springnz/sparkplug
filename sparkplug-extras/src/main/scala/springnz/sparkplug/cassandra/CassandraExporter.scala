package springnz.sparkplug.cassandra

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.RowWriterFactory
import springnz.sparkplug.core.SparkOperation
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import springnz.sparkplug.util.{ Pimpers, Logging }
import Pimpers._
import springnz.sparkplug.util.Logging

import scala.util.Try
import scalaz.syntax.bind._

object CassandraExporter
    extends Logging with Serializable {

  CustomTypeConverters.registerTimeConverters()

  def apply[A: RowWriterFactory](dataSource: SparkOperation[RDD[A]],
    keySpace: String,
    table: String,
    createQueries: List[String] = Nil): SparkOperation[(RDD[A], Try[Unit])] = {

    def createTableIfNotExists(ctx: SparkContext) =
      createQueries.foreach { query ⇒
        val firstLine = query.lines.toList.headOption.getOrElse("")
        log.info(s"Executing query: '$firstLine' ...")
        CassandraConnector(ctx.getConf).withSessionDo(_.execute(query))
      }

    val savedData: SparkOperation[Try[Unit]] =
      for (rdd ← dataSource) yield {
        Try {
          createTableIfNotExists(rdd.sparkContext)
          log.info(s"Saving RDD to Cassandra with ${rdd.count()} rows")
          rdd.saveToCassandra(keySpace, table)
        }.withErrorLog("Error executing operation:")
      }

    // return the SparkOperation[Unit] to indicate an effect
    (dataSource ⊛ savedData).tupled
  }
}

