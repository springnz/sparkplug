//TODO: add unit tests in this project

package springnz.sparkplug.data

import org.apache.spark.sql.{ DataFrame, SaveMode }
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.util.Logging
import springnz.sparkplug.util.Pimpers._

import scala.util.Try

object JdbcDataFrameExporter extends Logging {

  def export(dataBaseName: String, tableName: String, saveMode: SaveMode)(dataFrame: DataFrame): SparkOperation[(DataFrame, Try[Unit])] =
    SparkOperation { ctx ⇒
      val writeResult = Try {
        val config = ctx.getConf
        val driverClass = config.get("spark.mysql.driverClass")
        Class.forName(driverClass)
        val connectionString = config.get(s"spark.mysql.$dataBaseName.connectionString")
        val user = config.get(s"spark.mysql.$dataBaseName.user")
        val password = config.get(s"spark.mysql.$dataBaseName.password")
        dataFrame.write
          .mode(SaveMode.Overwrite)
          .jdbc(connectionString, tableName, Map("user" -> user, "password" -> password))
      }.withErrorLog(s"Failed to write to JDBC database '$dataBaseName', table '$tableName'")
      (dataFrame, writeResult)
    }
}
