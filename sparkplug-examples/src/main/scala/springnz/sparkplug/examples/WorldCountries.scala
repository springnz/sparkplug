package springnz.sparkplug.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.json4s._
import org.json4s.jackson.Serialization.read
import springnz.sparkplug.core._
import springnz.sparkplug.data.JdbcDataFrameSource
import springnz.sparkplug.examples.WorldDataTypes._

import scala.util.Try

object WorldPipeline {
  implicit val formats = DefaultFormats

}
trait WorldPipeline {
  import WorldPipeline._

  def dataSource: SparkOperation[DataFrame] = JdbcDataFrameSource("world", "Country")

  lazy val countriesOperation: SparkOperation[RDD[Country]] = for {
    dataFrame ← dataSource
  } yield {
    dataFrame.toJSON.mapWithFilter {
      jsonString ⇒ Try { read[Country](jsonString) }.toOption
    }
  }
}

