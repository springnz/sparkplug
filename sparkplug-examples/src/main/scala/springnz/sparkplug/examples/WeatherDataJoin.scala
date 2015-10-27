package springnz.sparkplug.examples

import springnz.sparkplug.core.SparkPimpers._
import springnz.sparkplug.core._
import springnz.sparkplug.data.SparkDataSource
import springnz.sparkplug.examples.WeatherDataTypes._
import springnz.util.Logging

object WeatherDataJoin extends LocalExecutable("WeatherDataJoin") with WeatherDataJoinPipeline {
  def main(args: Array[String]): Unit = {
    val executor = new SparkExecutor {
      override val configurer: Configurer = new LocalConfigurer("WeatherDataJoin")
    }
    executor.execute(weatherDataJoin)
  }
}

trait WeatherDataJoinPipeline extends Logging {

  lazy val rawWeatherDataSource: SparkDataSource[RawWeatherData] = new RawWeatherSource
  lazy val stationSource: SparkDataSource[StationData] = new StationSource

  lazy val rawWeatherFeed = rawWeatherDataSource().map {
    inputRDD ⇒ inputRDD.mapWithFilter { data ⇒ if (data.wsid.nonEmpty) Some((data.wsid, data)) else None }
  }

  lazy val stationFeed = for (inputRDD ← stationSource()) yield {
    inputRDD.map(data ⇒ (data.id, data))
  }

  lazy val weatherDataJoin = for {
    stationRDD ← stationFeed
    rawWeatherRDD ← rawWeatherFeed
  } yield {
    val joinedRDD = stationRDD.join(rawWeatherRDD)
    log.debug(s"$joinedRDD.count rows had the same station id")
    joinedRDD
  }

}

