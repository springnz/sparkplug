package springnz.sparkplug.cassandra

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import springnz.sparkplug.cassandra.CassandraTypes.KeySpace
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.data.SparkDataSource
import org.apache.spark.rdd.RDD
import springnz.util.Logging

import scala.reflect.ClassTag

class CassandraRDDSource[A: ClassTag: RowReaderFactory: ValidRDDType](keySpace: KeySpace, table: String)
    extends SparkDataSource[A] with Logging {

  CustomTypeConverters.registerTimeConverters()

  override def apply(): SparkOperation[RDD[A]] =
    SparkOperation { ctx ⇒
      log.info(s"Fetching RDD from keyspace='$keySpace' table='$table'...")
      ctx.cassandraTable[A](keySpace.keySpace, table)
    }
}

