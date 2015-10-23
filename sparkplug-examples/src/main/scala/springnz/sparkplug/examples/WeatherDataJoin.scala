package springnz.sparkplug.examples

import org.apache.spark.rdd.RDD
import springnz.sparkplug.core.SparkPimpers._
import springnz.sparkplug.core._
import springnz.sparkplug.data.{ SparkDataSource, SparkRDDProcess }
import springnz.sparkplug.examples.WeatherDataTypes._
import springnz.util.Logging

object WeatherDataJoin extends LocalExecutable("WeatherDataJoin") with WeatherDataJoinPipeline {
  def main(args: Array[String]): Unit = {
    val executor = new SparkExecutor {
      override val configurer: Configurer = new LocalConfigurer("WeatherDataJoin")
    }
    executor.execute(weatherDataJoin())
  }
}

class StationFeed(stationDataSource: SparkDataSource[StationData])
    extends SparkRDDProcess[(String, StationData)] with Logging {

  override def apply() =
    for (inputRDD ← stationDataSource()) yield {
      inputRDD.map(data ⇒ (data.id, data))
    }
}

class RawWeatherFeed(rawWeatherDataSource: SparkDataSource[RawWeatherData])
    extends SparkRDDProcess[(String, RawWeatherData)] with Logging {

  override def apply(): SparkOperation[RDD[(String, RawWeatherData)]] =
    rawWeatherDataSource().map {
      inputRDD ⇒ inputRDD.mapWithFilter { data ⇒ if (data.wsid.nonEmpty) Some((data.wsid, data)) else None }
    }
}

class WeatherDataJoin(stationFeed: StationFeed, rawWeatherFeed: RawWeatherFeed) extends Logging {
  def apply() = for {
    stationRDD ← stationFeed()
    rawWeatherRDD ← rawWeatherFeed()
  } yield {
    val joinedRDD = stationRDD.join(rawWeatherRDD)
    log.debug(s"$joinedRDD.count rows had the same station id")
    joinedRDD
  }
}

trait WeatherDataJoinPipeline {
  import com.softwaremill.macwire._

  lazy val rawWeatherDataSource: SparkDataSource[RawWeatherData] = wire[RawWeatherSource]
  lazy val stationSource: SparkDataSource[StationData] = wire[StationSource]
  lazy val rawWeatherFeed = wire[RawWeatherFeed]
  lazy val stationFeed = wire[StationFeed]
  lazy val weatherDataJoin = wire[WeatherDataJoin]
}

