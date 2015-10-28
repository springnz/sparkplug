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

    def persist(rddName: String)(implicit projectName: ProjectName): RDD[A] = {
      val path = RDDPersister.getPath(projectName.name, rddName).fullPath
      RDDPersister.persistRDD(path, rdd)
    }

    def sampleAndPersist(
      rddName: String,
      params: RDDSamplerParams,
      overwrite: Boolean = false)(implicit projectName: ProjectName, log: Logger): RDD[A] = {
      val path = RDDPersister.getPath(projectName.name, rddName)
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
