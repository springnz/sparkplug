package springnz.sparkplug.data

import com.typesafe.config.ConfigFactory
import springnz.sparkplug.core.SparkConverters._
import springnz.sparkplug.core.{ SparkOperation, SparkProcess }
import org.apache.spark.sql.{ DataFrame, SaveMode }
import springnz.util.Pimpers._

import scala.util.Try

class JdbcDataFrameExporter(dataFrame: SparkProcess[DataFrame], dataBaseName: String, tableName: String, saveMode: SaveMode) {

  protected val config = ConfigFactory.load()

  def apply(): SparkOperation[(DataFrame, Try[Unit])] = for {
    dataFrame ← dataFrame()
  } yield {

    val writeResult = Try {
      Class.forName(config.getString("spark.mysql.driverClass"))
      val connectionString = config.getString(s"spark.mysql.$dataBaseName.connectionString")
      val user = config.getString(s"spark.mysql.$dataBaseName.user")
      val password = config.getString(s"spark.mysql.$dataBaseName.password")
      dataFrame.write
        .mode(SaveMode.Overwrite)
        .jdbc(connectionString, tableName, Map("user" -> user, "password" -> password))
    }.withErrorLog(s"Failed to write to JDBC database '$dataBaseName', table '$tableName'")
    (dataFrame, writeResult)
  }

}

