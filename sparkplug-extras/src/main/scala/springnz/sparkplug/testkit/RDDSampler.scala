package springnz.sparkplug.testkit

import java.lang.Math._

import com.typesafe.config.ConfigFactory
import org.apache.spark.partial.{ BoundedDouble, PartialResult }
import org.apache.spark.rdd.RDD
import springnz.sparkplug.util.SerializeUtils
import springnz.util.Logging

import scala.reflect.ClassTag

case class RDDSamplerParams(
  testerFraction: Double,
  scaleParam: Double,
  scalePower: Double,
  minimum: Double,
  sequential: Boolean)

object RDDSampler extends Logging {

  val config = ConfigFactory.load()

  private[sparkplug] def shrinkFactor(
    testerFraction: Double,
    scaleParam: Double,
    scalePower: Double,
    minimum: Double,
    testerLength: Double): Double = {

    val fullLength = testerLength / testerFraction
    val calcFrac = Math.pow(fullLength, scalePower) / fullLength * scaleParam
    // don't bother shrinking to less than the minimum, but cap at 1.0
    min(if (minimum > 0) max(calcFrac, minimum / fullLength) else calcFrac, 1.0)
  }

  private[sparkplug] def sample[A: ClassTag](rdd: RDD[A], dataSourceType: String): RDD[A] = {
    val testerFraction = config.getDouble(s"sampling.$dataSourceType.testerfraction")
    val scaleParam = config.getDouble(s"sampling.$dataSourceType.scaleparam")
    val scalePower = config.getDouble(s"sampling.$dataSourceType.scalepower")
    val minimum = config.getDouble(s"sampling.$dataSourceType.minimum")
    val sequential = config.getBoolean(s"sampling.$dataSourceType.sequential")
    sample(rdd, RDDSamplerParams(
      testerFraction = testerFraction, scaleParam = scaleParam, scalePower = scalePower, minimum = minimum,
      sequential = sequential))
  }

  private[sparkplug] def sample[A: ClassTag](rdd: RDD[A], params: RDDSamplerParams): RDD[A] = {

    def getSample(params: RDDSamplerParams): RDD[A] = {
      val approxSize: PartialResult[BoundedDouble] = rdd.countApprox(60000, 0.95)
      val sampleLength = approxSize.initialValue.mean * params.testerFraction
      if (sampleLength < 50) {
        // take a bigger sample
        val updatedTesterFraction = params.testerFraction * 50 / sampleLength
        getSample(params.copy(testerFraction = updatedTesterFraction))
      } else {
        val sample = rdd.take(sampleLength.toInt)
        val tester = SerializeUtils.serialize(sample)
        val sampleFraction = shrinkFactor(params.testerFraction, params.scaleParam, params.scalePower,
          params.minimum, tester.length)
        val fullCount = sample.length / params.testerFraction

        val reSampled = if (params.sequential)
          rdd.sparkContext.parallelize(rdd.take((fullCount * sampleFraction).toInt), 10)
        else
          rdd.sample(withReplacement = true, sampleFraction, 0)
        reSampled
      }
    }

    if (params.scaleParam == 1.0 && params.scalePower == 1.0)
      rdd
    else
      getSample(params)
  }

}
