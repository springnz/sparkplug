package springnz.sparkplug.testkit

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.data.SparkRDDProcess
import springnz.util.Logging

import scala.reflect.ClassTag

@deprecated()
class PersistableRDDProcess[A: ClassTag: RowReaderFactory: ValidRDDType](
  projectFolder: String, processName: String, takeSample: Boolean = true)(rddProcess: SparkRDDProcess[A])
    extends SparkRDDProcess[A] with Logging {

  import RDDPersister._
  import RDDSampler._
  val dataSourceType = "derived-rdd"

  val (tablePath, fileExists) = cacheFileNameAndStatus(projectFolder, dataSourceType, processName, rddProcess.getClass.getName)

  override def apply() = SparkOperation { ctx ⇒
    if (fileExists) {
      log.debug(s"Fetching RDD from $tablePath ...")
      ctx.objectFile[A](tablePath)
    } else {
      def operation = rddProcess.apply()
      val rdd = operation.run(ctx)
      val usedSample = if (takeSample) {
        log.info("Taking sample ...")
        sample(rdd, dataSourceType)
      } else rdd
      persistRDD(tablePath, usedSample)
    }

  }
}

