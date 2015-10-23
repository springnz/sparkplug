package springnz.sparkplug

import org.scalatest.{ ShouldMatchers, WordSpec }
import springnz.sparkplug.examples.WeatherDataTypes._
import springnz.sparkplug.examples._
import springnz.sparkplug.testkit.CassandraTestRDDSource
import springnz.util.Logging

class WeatherTests extends WordSpec with ShouldMatchers with Logging {
  import CassandraConstants._

  "RawWeatherCount" should {
    "return success with the number of rows" in new CassandraTestContext("RawWeatherCount") {

      val rowCountResult = execute((new SimpleNonDIExample() {
        override def weatherDataSource = new CassandraTestRDDSource[RawWeatherData](projectFolder = projectFolder, keySpace = weatherKeySpace, table = rawWeatherData)
      })())
      rowCountResult.get.toInt should be > 0
    }
  }

  // This test case uses the MacWire DI framework to create and wire the dependencies
  "RawWeatherEvents" should {
    "return success with the first 10 URLs in alphabetical order" in new CassandraTestContext("RawWeatherEvents") with SimpleDIPipeline {
      override lazy val weatherDataProvider = new CassandraTestRDDSource[RawWeatherData](projectFolder = projectFolder, keySpace = weatherKeySpace, table = rawWeatherData)
      val rowCountResult = execute(dIExample())
      val rows = rowCountResult.get
      log.debug(s"${rows.length} rows found.")
      rows should have length 10
    }
  }

  "StationLatLon" should {
    "return an RDD with all the solver lat/longs" in new CassandraTestContext("StationLatLon") {
      val latLongOperation = (new StationLatLon {
        override def stationDataSource = new CassandraTestRDDSource[StationData](projectFolder = projectFolder, keySpace = weatherKeySpace, table = weatherStations)
      })()
      val latLongResult = execute(latLongOperation map { latLongRDD ⇒
        log.debug(s"Produced an RDD of size $latLongRDD.count")
        // turn it into something we can inspect
        latLongRDD.collect().toList
      })

      val latLongList = latLongResult.get
      latLongList.length should be > 0
      log.info(s"Produced a list of LatLong, starting with: ${latLongList.head}")
    }
  }
}

