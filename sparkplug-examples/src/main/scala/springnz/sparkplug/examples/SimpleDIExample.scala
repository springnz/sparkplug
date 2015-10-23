package springnz.sparkplug.examples

import springnz.sparkplug.cassandra.CassandraRDDSource
import springnz.sparkplug.core._
import springnz.sparkplug.data.SparkDataSource
import springnz.sparkplug.examples.WeatherDataTypes.RawWeatherData
import springnz.util.Logging

object SimpleDIExample extends LocalExecutable("SimpleDIExample") with SimpleDIPipeline with Logging {
  import CassandraConstants._
  def main(args: Array[String]): Unit = {
    executor.execute(dIExample())
  }

  class RawWeatherSource extends CassandraRDDSource[RawWeatherData](weatherKeySpace, rawWeatherData)
}

class SimpleDIExample(weatherDataSource: SparkDataSource[RawWeatherData]) extends Logging {

  def apply() = for {
    rdd ← weatherDataSource()
    firstData = rdd.map { data ⇒ (data.wsid, data.temperature) }.takeOrdered(10).toList // this can be in yield
  } yield {
    log.debug(s"First 10 URLs are: $firstData")
    firstData
  }
}

trait SimpleDIPipeline {
  import SimpleDIExample._
  import com.softwaremill.macwire._
  lazy val weatherDataProvider: SparkDataSource[RawWeatherData] = wire[RawWeatherSource]
  lazy val dIExample = wire[SimpleDIExample]
}
