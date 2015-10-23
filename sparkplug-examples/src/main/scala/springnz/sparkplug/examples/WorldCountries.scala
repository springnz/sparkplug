package springnz.sparkplug.examples

import org.apache.spark.sql.DataFrame
import play.api.libs.json._
import springnz.sparkplug.core._
import springnz.sparkplug.data.{ SparkRDDProcess, JdbcDataFrameSource }
import springnz.sparkplug.examples.WorldCountries.DataSourceFunction
import springnz.sparkplug.examples.WorldDataTypes._
import springnz.util.Logging

object WorldCountries extends LocalExecutable("WorldCountries") with WorldPipeline {
  def main(args: Array[String]): Unit = {
    val executor = new SparkExecutor {
      override val configurer: Configurer = new LocalConfigurer("WorldCountries")
    }

    executor.execute(countriesQuery())
  }

  class DataSourceFunction extends (String ⇒ SparkOperation[DataFrame]) {
    def apply(table: String) = (new JdbcDataFrameSource("world", table))()
  }

}

class WorldCountries(datasource: DataSourceFunction)
    extends SparkRDDProcess[Country] with Logging {
  import SparkPimpers._

  def apply() = for {
    dataFrame ← datasource("Country")
  } yield {
    dataFrame.toJSON.mapWithFilter {
      jsonString ⇒
        val country = Json.parse(jsonString).validate[Country]
        country.asOpt
    }
  }
}

trait WorldPipeline {
  import com.softwaremill.macwire._

  lazy val dataSourceFunction: DataSourceFunction = wire[DataSourceFunction]
  lazy val countriesQuery: WorldCountries = wire[WorldCountries]
}

