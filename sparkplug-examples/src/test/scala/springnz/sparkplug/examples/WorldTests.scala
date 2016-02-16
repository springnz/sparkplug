package springnz.sparkplug.examples

import org.scalatest._
import springnz.sparkplug.examples.WorldDataTypes.Country
import springnz.sparkplug.testkit.SimpleTestContext
import springnz.sparkplug.util.Logging

class WorldTests extends WordSpec with ShouldMatchers with Logging {
  "World Jdbc Feed" should {
    "return the columns and the number of countries" in new SimpleTestContext("WorldTests") with WorldTestPipeline {

      val (countries, count) = execute(countriesOperation.takeOrderedWithCount(20)(Ordering.by(_.Name))).get
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
