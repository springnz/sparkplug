package springnz.sparkplug

import org.scalatest._
import springnz.sparkplug.core.SparkConverters._
import springnz.sparkplug.examples.WorldCountries.DataSourceFunction
import springnz.sparkplug.examples.WorldDataTypes.Country
import springnz.sparkplug.examples.{ WorldCountries, WorldPipeline }
import springnz.sparkplug.testkit.JdbcTestDataSource
import springnz.util.Logging

class WorldTests extends WordSpec with ShouldMatchers with Logging {
  "World Jdbc Feed" should {
    "return the columns and the number of countries" in new SimpleTestContext("WorldTests") with WorldTestPipeline {

      val countriesProcess = new WorldCountries(dataSourceFunction)

      val (countries, count) = execute(countriesProcess().takeOrderedWithCount(20)(Ordering.by(country ⇒ country.Name))).get
      val first = countries.head
      first should be(Country("AFG",
        "Afghanistan",
        "Asia",
        "Southern and Central Asia",
        652090.0,
        Some(1919),
        22720000,
        Some(45.9),
        Some(5976.0),
        None, "Afganistan/Afqanestan",
        "Islamic Emirate",
        Some("Mohammad Omar"),
        Some(1),
        "AF"))

      count should be(239)
    }
  }
}

trait WorldTestPipeline extends WorldPipeline {
  override lazy val dataSourceFunction = new DataSourceFunction {
    override def apply(table: String) = (new JdbcTestDataSource("sparkplug-core", "world", "Country", sample = false))()
  }

}

