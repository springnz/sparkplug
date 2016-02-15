package springnz.sparkplug.examples

import org.apache.spark.rdd.RDD
import springnz.sparkplug.cassandra.CassandraRDDFetcher
import springnz.sparkplug.core._
import springnz.sparkplug.examples.CassandraConstants._
import springnz.sparkplug.examples.WeatherDataTypes.RawWeatherData
import springnz.sparkplug.util.Logging

object SimplePipelineExample extends LocalExecutable("SimpleDIExample") with SimplePipeline with Logging {
  def main(args: Array[String]): Unit = {
    executor.execute(dIExample)
  }
}

trait SimplePipeline extends Logging {
  def weatherDataSource: SparkOperation[RDD[RawWeatherData]] =
    CassandraRDDFetcher.selectAll[RawWeatherData](weatherKeySpace, rawWeatherData)

  lazy val dIExample: SparkOperation[List[(String, Double)]] = for {
    rdd ← weatherDataSource
    firstData = rdd.map { data ⇒ (data.wsid, data.temperature) }.takeOrdered(10).toList // this can be in yield
  } yield {
    log.debug(s"First 10 WeatherPoints are: $firstData")
    firstData
  }
}

