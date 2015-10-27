package springnz.sparkplug.testkit

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RDDTestSampleExtensions {

  implicit class RDDTestSampler[A: ClassTag: RowReaderFactory: ValidRDDType](rdd: RDD[A]) {

    def sample(params: RDDSamplerParams): RDD[A] =
      RDDSampler.sample(rdd, params)

    def persist(projectName: String, rddName: String): RDD[A] = {
      val path = RDDPersister.getPath(projectName, rddName).fullPath
      RDDPersister.persistRDD(path, rdd)
    }

    def sampleAndPersist(
      projectName: String,
      rddName: String,
      params: RDDSamplerParams,
      overwrite: Boolean = false)(implicit log: Logger): RDD[A] = {
      val path = RDDPersister.getPath(projectName, rddName)
      if (overwrite || (!overwrite && !path.exists)) {
        val rddSample = sample(params)
        RDDPersister.persistRDD(path.fullPath, rddSample)
      } else { // (!overwrite && path.exists)
        log.info(s"Not sampling/persisting RDD that already exists at path [${path.fullPath}]")
        rdd
      }

    }
  }
}
