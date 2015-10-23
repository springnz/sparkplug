package springnz.sparkplug.examples

import springnz.sparkplug.cassandra.CassandraRDDSource
import org.apache.spark.rdd.RDD
import springnz.sparkplug.core._
import springnz.sparkplug.data.SparkDataSource
import springnz.sparkplug.examples.WeatherDataTypes.StationData
import springnz.util.Logging

object StationLatLon extends LocalExecutable("StationLatLon") {
  def main(args: Array[String]): Unit = {
    val executor = new SparkExecutor {
      override val configurer: Configurer = new LocalConfigurer("StationLatLon")
    }
    executor.execute((new StationLatLon)())
  }

  // note the incorrect spelling of latitude
  case class LatLong(lat: Double, lon: Double)
}

class StationLatLon extends Logging {
  import CassandraConstants._
  import StationLatLon._

  protected def stationDataSource: SparkDataSource[StationData] =
    new CassandraRDDSource[StationData](weatherKeySpace, rawWeatherData)

  def apply(): SparkOperation[RDD[LatLong]] = stationDataSource().map { inputRDD ⇒
    val rdd = inputRDD.map(data ⇒ (data.countryCode, LatLong(data.lat, data.long)))
      .collect { case (Some(_), data) ⇒ data }
      .filter { case LatLong(lat, lon) ⇒ lat != 0.0 || lon != 0.0 }
    rdd
  }
}

