package springnz.sparkplug.testkit

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import springnz.sparkplug.cassandra.CassandraRDDSource
import springnz.sparkplug.cassandra.CassandraTypes.KeySpace

import scala.reflect.ClassTag

class CassandraTestRDDSource[A: ClassTag: RowReaderFactory: ValidRDDType](
  projectFolder: String, keySpace: KeySpace, table: String, takeSample: Boolean = true, persistSample: Boolean = true)
    extends SparkTestRDDSource[A](
      new CassandraRDDSource[A](keySpace, table))(
      projectFolder,
      "cassandra",
      keySpace.keySpace,
      table,
      takeSample,
      persistSample) {
}
