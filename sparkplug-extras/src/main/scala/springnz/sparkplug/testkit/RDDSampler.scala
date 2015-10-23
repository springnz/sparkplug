package springnz.sparkplug.testkit

import java.lang.Math._

import com.typesafe.config.ConfigFactory
import springnz.sparkplug.util.SerializeUtils
import org.apache.spark.partial.{ BoundedDouble, PartialResult }
import org.apache.spark.rdd.RDD
import springnz.util.Logging

import scala.reflect.ClassTag

object RDDSampler extends Logging {

  val config = ConfigFactory.load()

  private[sparkplug] def shrinkFactor(testerFrac: Double,
    scaleParam: Double,
    scalePower: Double,
    minimum: Double,
    testerLength: Double): Double = {

    val fullLength = testerLength / testerFrac
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

    lazy val approxSize: PartialResult[BoundedDouble] = rdd.countApprox(60000, 0.95)

    def getSample(rdd: RDD[A], testerFraction: Double, scaleParam: Double, scalePower: Double, sequential: Boolean): RDD[A] = {

      val sampleLength = approxSize.initialValue.mean * testerFraction
      if (sampleLength < 50) // take a bigger sample
        getSample(rdd, testerFraction * 50 / sampleLength, scaleParam, scalePower, sequential)
      else {

        val sample = rdd.take(sampleLength.toInt)
        val tester = SerializeUtils.serialize(sample)
        val sampleFraction = shrinkFactor(testerFraction, scaleParam, scalePower, minimum, tester.length)
        val fullCount = sample.length / testerFraction

        val reSampled = if (sequential)
          rdd.sparkContext.parallelize(rdd.take((fullCount * sampleFraction).toInt), 10)
        else
          rdd.sample(withReplacement = true, sampleFraction, 0)
        reSampled
      }
    }

    if (scaleParam == 1.0 && scalePower == 1.0) rdd else
      getSample(rdd, testerFraction, scaleParam, scalePower, sequential)
  }

}
