package springnz.sparkplug.examples

import org.scalatest.{ ShouldMatchers, WordSpec }
import springnz.sparkplug.testkit.SimpleTestContext
import springnz.sparkplug.util.Logging

class WeatherTests extends WordSpec with ShouldMatchers with Logging {
  import springnz.sparkplug.testkit._

  "RawWeatherCount" should {
    "return success with the number of rows" in new SimpleTestContext("RawWeatherCount") {

      val rowCountResult = execute((new SimpleNonDIExample() {
        override def weatherDataSource = super.weatherDataSource.sourceFrom("RawWeatherData")
      })())
      rowCountResult.get.toInt should be > 0
    }
  }

  // This test case uses the MacWire DI framework to create and wire the dependencies
  "RawWeatherEvents" should {
    "return success with the first 10 URLs in alphabetical order" in new SimpleTestContext("RawWeatherEvents") with SimplePipeline {
      override lazy val weatherDataSource = super.weatherDataSource.sourceFrom("RawWeatherData")
      val rowCountResult = execute(dIExample)
      val rows = rowCountResult.get
      log.debug(s"${rows.length} rows found.")
      rows should have length 10
    }
  }

  "StationLatLon" should {
    "return an RDD with all the solver lat/longs" in new SimpleTestContext("StationLatLon") {
      val latLongOperation = (new StationLatLon {
        override def stationDataSource = super.stationDataSource.sourceFrom("StationData")
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

