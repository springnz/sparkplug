package springnz.sparkplug.examples

import springnz.sparkplug.cassandra.CassandraRDDFetcher
import springnz.sparkplug.core._
import springnz.sparkplug.examples.CassandraConstants._
import springnz.sparkplug.examples.WeatherDataTypes.RawWeatherData
import springnz.sparkplug.util.Logging

object SimpleNonDIExample extends LocalExecutable("SimpleNonDIExample") {
  def main(args: Array[String]): Unit = {
    val operation = new SimpleNonDIExample()
    executor.execute(operation())
  }
}

class SimpleNonDIExample() extends Logging {

  // As we are not using DI, the
  protected def weatherDataSource = CassandraRDDFetcher.selectAll[RawWeatherData](weatherKeySpace, rawWeatherData)

  def apply(): SparkOperation[Long] = for {
    rdd ← weatherDataSource
  } yield {
    val count = rdd.count
    log.info(s"record count is $count")
    count
  }
}
