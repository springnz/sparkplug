package springnz.sparkplug.examples

import springnz.sparkplug.cassandra.CassandraRDDSource
import springnz.sparkplug.core._
import springnz.sparkplug.data.SparkDataSource
import springnz.sparkplug.examples.CassandraConstants._
import springnz.sparkplug.examples.WeatherDataTypes.RawWeatherData
import springnz.util.Logging

object SimplePipelineExample extends LocalExecutable("SimpleDIExample") with SimplePipeline with Logging {
  def main(args: Array[String]): Unit = {
    executor.execute(dIExample)
  }
}

trait SimplePipeline extends Logging {
  lazy val weatherDataSource: SparkDataSource[RawWeatherData] =
    new CassandraRDDSource[RawWeatherData](weatherKeySpace, rawWeatherData)

  lazy val dIExample: SparkOperation[List[(String, Double)]] = for {
    rdd ← weatherDataSource()
    firstData = rdd.map { data ⇒ (data.wsid, data.temperature) }.takeOrdered(10).toList // this can be in yield
  } yield {
    log.debug(s"First 10 WeatherPoints are: $firstData")
    firstData
  }
}

