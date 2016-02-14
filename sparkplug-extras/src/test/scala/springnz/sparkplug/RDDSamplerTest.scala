package springnz.sparkplug

import org.scalatest.prop.PropertyChecks
import org.scalatest.{ ShouldMatchers, WordSpec }
import springnz.sparkplug.testkit.RDDSamplers
import springnz.sparkplug.util.Logging

class RDDSamplerTest extends WordSpec with ShouldMatchers with PropertyChecks with Logging {

  case class ShrinkParams(testerFraction: Double, scaleParam: Double, scalePower: Double, testerLength: Double, minimumLength: Double)

  def getShrinkFactorAndSize(sp: ShrinkParams) = {
    val shrinkFactor = RDDSamplers.shrinkFactor(sp.testerFraction, sp.scaleParam, sp.scalePower, sp.minimumLength, sp.testerLength)
    val fullSize = sp.testerLength / sp.testerFraction
    val shrunkSize = shrinkFactor * fullSize
    val formatter = java.text.NumberFormat.getIntegerInstance
    log.info(f"Scale: ${sp.scaleParam}%1.0f, Power: ${sp.scalePower}%1.4f, DataSet: $fullSize%1.0f, Shrunk: $shrunkSize%1.0f")
    (shrinkFactor, shrunkSize)
  }

  "CassandraTestRDDTest" should {
    "calculate reasonable shrink factors" in {
      val scaleFactor = 1000
      val power = 0.30102999566398
      val testerFrac = 0.001

      val shrinkParamList = List(
        ShrinkParams(testerFrac, scaleFactor, power, 1000000000, 0.0), // 1TB data
        ShrinkParams(testerFrac, scaleFactor, power, 100000000, 0.0), // 100GB data
        ShrinkParams(testerFrac * 10, scaleFactor, power, 1000000000, 0.0), // 100GB data
        ShrinkParams(testerFrac, scaleFactor, power, 10000000, 0.0), // 10GB data
        ShrinkParams(testerFrac, scaleFactor, power, 1000000, 0.0), // 1GB data
        ShrinkParams(testerFrac, scaleFactor, power, 100000, 0.0), // 100MB data
        ShrinkParams(testerFrac, scaleFactor, power, 10000, 0.0), // 10MB data
        ShrinkParams(testerFrac, scaleFactor, power, 1000, 0.0), // 1MB data
        ShrinkParams(testerFrac * 1000, scaleFactor, power, 1000000, 0.0), // 1MB data
        ShrinkParams(testerFrac, scaleFactor, power, 100000, 10000000), // 100MB data, floored at 10MB
        ShrinkParams(testerFrac, scaleFactor, power, 10000, 10000000), // 10MB data, floored at 10MB
        ShrinkParams(testerFrac, scaleFactor, power, 1000, 10000000) // 1MB data, floored at 10MB
        )
      val shrinkFactor = shrinkParamList map getShrinkFactorAndSize

      shrinkFactor should be(List(
        (4.095999999999862E-6, 4095999.999999862),
        (2.0479999999999367E-5, 2047999.9999999367),
        (2.0479999999999367E-5, 2047999.9999999367),
        (1.0239999999999712E-4, 1023999.9999999712),
        (5.119999999999871E-4, 511999.9999999871),
        (0.0025599999999999425, 255999.99999999427),
        (0.012799999999999749, 127999.9999999975),
        (0.06399999999999893, 63999.99999999893),
        (0.06399999999999893, 63999.99999999893),
        (0.1, 1.0E7),
        (1.0, 1.0E7),
        (1.0, 1000000.0)))
    }
  }
}
