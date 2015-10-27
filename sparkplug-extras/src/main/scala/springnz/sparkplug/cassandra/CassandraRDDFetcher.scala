package springnz.sparkplug.cassandra

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import org.apache.spark.rdd.RDD
import springnz.sparkplug.cassandra.CassandraTypes.{ KeySpace, Table }
import springnz.sparkplug.core.SparkOperation
import springnz.util.Logging

import scala.reflect.ClassTag

class CassandraRDDFetcher[A: ClassTag: RowReaderFactory: ValidRDDType](keySpace: KeySpace, table: Table)
    extends Logging {

  CustomTypeConverters.registerTimeConverters()

  def selectAll(): SparkOperation[RDD[A]] =
    SparkOperation { ctx ⇒
      log.info(s"Fetching RDD from keyspace='$keySpace' table='$table' (full table)...")
      ctx.cassandraTable[A](keySpace.name, table.name)
    }

  def selectWhere(cql: String, values: Any*): SparkOperation[RDD[A]] =
    SparkOperation { ctx ⇒
      log.info(s"Fetching RDD from keyspace='$keySpace' table='$table' with filter [$cql] [$values] ...")
      ctx.cassandraTable[A](keySpace.name, table.name).where(cql, values: _*)
    }

}
