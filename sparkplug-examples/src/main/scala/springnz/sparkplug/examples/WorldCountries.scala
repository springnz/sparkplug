package springnz.sparkplug.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import play.api.libs.json._
import springnz.sparkplug.core._
import springnz.sparkplug.data.JdbcDataFrameSource
import springnz.sparkplug.examples.WorldDataTypes._

trait WorldPipeline {
  import SparkPimpers._

  class DataSourceFunction(table: String) extends SparkProcess[DataFrame] {
    def apply() = (new JdbcDataFrameSource("world", table))()
  }

  def dataSource: SparkProcess[DataFrame] = new DataSourceFunction("Country")

  lazy val countriesOperation: SparkOperation[RDD[Country]] = for {
    dataFrame ← dataSource()
  } yield {
    dataFrame.toJSON.mapWithFilter {
      jsonString ⇒
        val country = Json.parse(jsonString).validate[Country]
        country.asOpt
    }
  }
}

