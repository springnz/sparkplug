package springnz.sparkplug.testkit

import java.lang.Math._

import org.apache.spark.partial.{ BoundedDouble, PartialResult }
import org.apache.spark.rdd.RDD

import springnz.sparkplug.util.SerializeUtils
import springnz.util.Logging

import scala.reflect.ClassTag

object RDDSamplers extends Logging {
  def identitySampler[A: ClassTag](rdd: RDD[A]) = rdd

  def defaultSampler[A: ClassTag](sampleParams: RDDSamplerParams = sourceRDDParams)(rdd: RDD[A]) = sample(rdd, sampleParams)

  val sourceRDDParams = RDDSamplerParams(
    testerFraction = 0.0001,
    scaleParam = 3000.0,
    scalePower = 0.30102999566398,
    minimum = 1000000.0,
    sequential = false)

  val derivedRDDParams = RDDSamplerParams(
    testerFraction = 0.1,
    scaleParam = 1.0,
    scalePower = 1.0,
    minimum = 1000000.0,
    sequential = false)

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

    if (params.scaleParam == 1.0 && params.scalePower == 1.0) {
      log.info("Not sampling RDD since scaleParam==scalePower==1.0")
      rdd
    } else {
      log.info(s"Sampling RDD with $params ...")
      getSample(params)
    }
  }

  case class RDDSamplerParams(
      testerFraction: Double,
      scaleParam: Double,
      scalePower: Double,
      minimum: Double,
      sequential: Boolean) {

    def withSequential(newSequential: Boolean): Unit = {
      RDDSamplerParams(testerFraction, scaleParam, scalePower, minimum, newSequential)
    }
  }

}

