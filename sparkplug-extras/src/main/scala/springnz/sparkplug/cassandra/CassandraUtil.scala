package springnz.sparkplug.cassandra

import com.datastax.driver.core.ResultSet
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.datastax.spark.connector.rdd.{ CassandraTableScanRDD, ValidRDDType }
import com.typesafe.scalalogging.Logger
import springnz.sparkplug.cassandra.CassandraTypes.{ KeySpace, Table }
import springnz.sparkplug.core.SparkOperation

import scala.reflect.ClassTag

object CassandraUtil {

  def dropTable(keyspace: String, table: String)(implicit log: Logger): SparkOperation[ResultSet] =
    SparkOperation { ctx ⇒
      CassandraConnector(ctx.getConf).withSessionDo { session ⇒
        log.info(s"Dropping table (if exists) $keyspace.$table ...")
        session.execute(s"DROP TABLE IF EXISTS $keyspace.$table")
      }
    }

  def count(keyspace: String, table: String)(implicit log: Logger): SparkOperation[Long] =
    SparkOperation { ctx ⇒
      val resultSet = CassandraConnector(ctx.getConf).withSessionDo { session ⇒
        log.info(s"Selecting row count from table $keyspace.$table ...")
        session.execute(s"SELECT COUNT(*) FROM $keyspace.$table")
      }
      resultSet.one().getLong(0)
    }

}
