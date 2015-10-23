package springnz.sparkplug.data

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{ DataFrame, SQLContext }
import springnz.sparkplug.core.SparkConverters._
import springnz.sparkplug.core.{ SparkOperation, SparkProcess }
import springnz.util.Logging

class JdbcDataFrameSource(dataBaseName: String, tableName: String)
    extends SparkProcess[DataFrame] with Logging {

  val config = ConfigFactory.load()
  val connectionString = config.getString(s"spark.mysql.$dataBaseName.connectionString")
  val user = config.getString(s"spark.mysql.$dataBaseName.user")
  val password = config.getString(s"spark.mysql.$dataBaseName.password")

  override def apply(): SparkOperation[DataFrame] = SparkOperation { ctx ⇒
    Class.forName(config.getString("spark.mysql.driverClass"))
    val sqlContext = new SQLContext(ctx)
    sqlContext.read.jdbc(connectionString, tableName, Map("user" -> user, "password" -> password))
  }
}
