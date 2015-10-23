package springnz.sparkplug.data

import java.sql.{ ResultSet, DriverManager }

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.{ JdbcRDD, RDD }
import springnz.sparkplug.core.SparkOperation
import springnz.util.Logging

import scala.reflect.ClassTag

class JdbcRDDSource[A: ClassTag: RowReaderFactory: ValidRDDType](
  dataBaseName: String,
  tableName: String,
  idField: String,
  lowerBound: Long,
  upperBound: Long,
  numPartitions: Int,
  conversion: ResultSet ⇒ A)
    extends SparkDataSource[A] with Logging {

  val config = ConfigFactory.load()
  val connectionString = config.getString(s"spark.mysql.$dataBaseName.connectionString")
  val user = config.getString(s"spark.mysql.$dataBaseName.user")
  val password = config.getString(s"spark.mysql.$dataBaseName.password")

  override def apply(): SparkOperation[RDD[A]] = SparkOperation { ctx ⇒
    def connection() = DriverManager.getConnection(connectionString, user, password)

    val rdd = new JdbcRDD(ctx, connection, s"select * from $tableName where ? <= $idField and $idField <= ?", lowerBound, upperBound, numPartitions, conversion)
    log.info(s"Fetched RDD from database='$dataBaseName', table='$tableName'")
    rdd
  }

}

