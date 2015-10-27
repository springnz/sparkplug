package springnz.sparkplug.cassandra

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import org.apache.spark.rdd.RDD
import springnz.sparkplug.cassandra.CassandraTypes.{ Table, KeySpace }
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.data.SparkDataSource
import springnz.util.Logging

import scala.reflect.ClassTag

@deprecated("Use CassandraRDDFetcher", "")
class CassandraRDDSource[A: ClassTag: RowReaderFactory: ValidRDDType](keySpace: KeySpace, table: Table)
    extends SparkDataSource[A] with Logging {

  val fetcher = new CassandraRDDFetcher[A](keySpace, table)

  override def apply(): SparkOperation[RDD[A]] = fetcher.selectAll()
}
