package springnz.sparkplug.examples

import springnz.sparkplug.cassandra.CassandraRDDSource
import springnz.sparkplug.core._
import springnz.sparkplug.data.SparkDataSource
import springnz.sparkplug.examples.CassandraConstants._
import springnz.sparkplug.examples.WeatherDataTypes.RawWeatherData
import springnz.util.Logging

object SimpleNonDIExample extends LocalExecutable("SimpleNonDIExample") {
  def main(args: Array[String]): Unit = {
    val operation = new SimpleNonDIExample()
    executor.execute(operation())
  }
}

class SimpleNonDIExample() extends Logging {

  // As we are not using DI, the
  protected def weatherDataSource: SparkDataSource[RawWeatherData] =
    new CassandraRDDSource[RawWeatherData](weatherKeySpace, rawWeatherData)

  def apply(): SparkOperation[Long] = for {
    rdd ← weatherDataSource()
  } yield {
    val count = rdd.count
    log.info(s"record count is $count")
    count
  }
}
