package springnz.sparkplug

import org.scalatest.{ ShouldMatchers, WordSpec }
import springnz.sparkplug.examples.CassandraConstants._
import springnz.sparkplug.examples.WeatherDataTypes._
import springnz.sparkplug.examples._
import springnz.sparkplug.testkit.CassandraTestRDDSource
import springnz.util.Logging

trait WeatherDataJoinTestPipeline extends WeatherDataJoinPipeline {
  override lazy val rawWeatherDataSource = new CassandraTestRDDSource[RawWeatherData](projectFolder = projectFolder, keySpace = weatherKeySpace, table = rawWeatherData)
  override lazy val stationSource = new CassandraTestRDDSource[StationData](projectFolder = projectFolder, keySpace = weatherKeySpace, table = weatherStations)
}

class PipelineTests extends WordSpec with ShouldMatchers with Logging with WeatherDataJoinTestPipeline {

  "WeatherDataFeed" should {
    "return success with an RDD of Weather Data" in new CassandraTestContext("VaultFeed") {
      // pipe the result into a type that can be inspected after the SparkContext is stopped
      val feedResult = execute(rawWeatherFeed() map {
        returnVal ⇒
          // turn it into something we can inspect
          log.debug(s"Produced an RDD of size ${returnVal.count}")
          returnVal.take(5)
      })
      feedResult.isSuccess shouldBe true
      val data = feedResult.get
      data should have size 5
    }
  }

  "WeatherDataJoin" should {
    "return success with the number of rows in the join table" in new CassandraTestContext("WeatherDataJoin") {
      val joinResult = execute(weatherDataJoin() map {
        returnVal ⇒
          // turn it into something we can inspect
          val count = returnVal.count
          log.debug(s"Produced an RDD of size $count")
          count
      })
      val rows: Long = joinResult.get
      println(joinResult.get)
    }
  }
}

