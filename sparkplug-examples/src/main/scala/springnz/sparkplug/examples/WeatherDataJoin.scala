package springnz.sparkplug.examples

import springnz.sparkplug.cassandra.CassandraRDDFetcher
import springnz.sparkplug.core._
import springnz.sparkplug.examples.CassandraConstants._
import springnz.sparkplug.examples.WeatherDataTypes._
import springnz.sparkplug.util.Logging

object WeatherDataJoin extends LocalExecutable("WeatherDataJoin") with WeatherDataJoinPipeline {
  def main(args: Array[String]): Unit = {
    val executor = new SparkExecutor {
      override val configurer: Configurer = new LocalConfigurer("WeatherDataJoin")
    }
    executor.execute(weatherDataJoin)
  }
}

trait WeatherDataJoinPipeline extends Logging {

  def rawWeatherDataSource = CassandraRDDFetcher.selectAll[RawWeatherData](weatherKeySpace, rawWeatherData)
  def stationSource = CassandraRDDFetcher.selectAll[StationData](weatherKeySpace, weatherStations)

  lazy val rawWeatherFeed = rawWeatherDataSource.map {
    inputRDD ⇒ inputRDD.mapWithFilter { data ⇒ if (data.wsid.nonEmpty) Some((data.wsid, data)) else None }
  }

  lazy val stationFeed = for (inputRDD ← stationSource) yield {
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

