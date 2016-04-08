package springnz.sparkplug.data

import org.apache.spark.sql.{ DataFrame, SQLContext }
import springnz.sparkplug.core.{ ConfigEnvironment, SparkOperation }
import springnz.sparkplug.util.{ Logging, Pimpers }

object JdbcDataFrameSource extends Logging {
  import Pimpers._

  def apply(dataBaseName: String, tableName: String): SparkOperation[DataFrame] = SparkOperation { ctx ⇒
    val config = ConfigEnvironment.config
    val connectionString = config.getString(s"spark.mysql.$dataBaseName.connectionString")
    val user = config.getString(s"spark.mysql.$dataBaseName.user")
    val password = config.getString(s"spark.mysql.$dataBaseName.password")

    Class.forName(config.getString("spark.mysql.driverClass"))
    val sqlContext = new SQLContext(ctx)

    sqlContext.read.jdbc(connectionString, tableName, Map("user" -> user, "password" -> password))
  }
}
