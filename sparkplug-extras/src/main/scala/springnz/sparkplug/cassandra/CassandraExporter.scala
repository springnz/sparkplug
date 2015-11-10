package springnz.sparkplug.cassandra

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.RowWriterFactory
import springnz.sparkplug.core.{ SparkOperation }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import springnz.util.Logging
import springnz.util.Pimpers._

import scala.util.Try
import scalaz.syntax.bind._

object CassandraExporter
    extends Logging with Serializable {

  CustomTypeConverters.registerTimeConverters()

  def apply[A: RowWriterFactory](dataSource: SparkOperation[RDD[A]],
    keySpace: String,
    table: String,
    createQuery: Option[String] = None): SparkOperation[(RDD[A], Try[Unit])] = {

    def createTableIfNotExists(ctx: SparkContext) =
      createQuery.foreach { query ⇒
        log.info(s"Creating table (if not exists) $keySpace.$table ...")
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

