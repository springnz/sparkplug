package springnz.sparkplug.testkit

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import springnz.sparkplug.cassandra.CassandraRDDSource
import springnz.sparkplug.cassandra.CassandraTypes.{ KeySpace, Table }

import scala.reflect.ClassTag

class CassandraTestRDDSource[A: ClassTag: RowReaderFactory: ValidRDDType](
  projectFolder: String, keySpace: KeySpace, table: Table, takeSample: Boolean = true, persistSample: Boolean = true)
    extends SparkTestRDDSource[A](
      new CassandraRDDSource[A](keySpace, table))(
      projectFolder,
      "cassandra",
      keySpace.name,
      table.name,
      takeSample,
      persistSample) {
}
