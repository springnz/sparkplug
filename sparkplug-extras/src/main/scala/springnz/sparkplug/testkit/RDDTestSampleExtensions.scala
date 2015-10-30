package springnz.sparkplug.testkit

import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RDDTestSampleExtensions {

  implicit class RDDTestSampler[A: ClassTag](rdd: RDD[A]) {

    def takeTestSample(params: RDDSamplerParams): RDD[A] =
      RDDSampler.sample(rdd, params)

    def persistTestResource(rddName: String, overwrite: Boolean = false)(
      implicit projectName: ProjectName, log: Logger): RDD[A] = {
      val path = RDDPersister.getPath(projectName.name, rddName)
      if (overwrite || (!overwrite && !path.exists)) {
        if (path.exists) {
          log.info(s"deleting existing RDD at ${path.fullPath}")
          path.delete()
        }
        RDDPersister.persistRDD(path.fullPath, rdd)
      } else { // (!overwrite && path.exists)
        log.info(s"Not persisting RDD that already exists at path [${path.fullPath}]")
        rdd
      }
    }

    def sampleAndPersistTestResource(
      rddName: String,
      params: RDDSamplerParams,
      overwrite: Boolean = false)(implicit projectName: ProjectName, log: Logger): RDD[A] = {
      val path = RDDPersister.getPath(projectName.name, rddName)
      if (overwrite || (!overwrite && !path.exists)) {
        if (path.exists) {
          log.info(s"deleting existing RDD at ${path.fullPath}")
          path.delete()
        }
        val rddSample = takeTestSample(params)
        RDDPersister.persistRDD(path.fullPath, rddSample)
      } else { // (!overwrite && path.exists)
        log.info(s"Not sampling/persisting RDD that already exists at path [${path.fullPath}]")
        rdd
      }
    }
  }
}
