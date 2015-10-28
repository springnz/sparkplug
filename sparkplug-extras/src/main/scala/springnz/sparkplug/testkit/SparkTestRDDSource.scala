package springnz.sparkplug.testkit

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import springnz.sparkplug.cassandra.CustomTypeConverters
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.data.SparkDataSource
import springnz.util.Logging

import scala.reflect.ClassTag
import scala.util.Try

@deprecated()
class SparkTestRDDSource[A: ClassTag: RowReaderFactory: ValidRDDType](
  sparkDsConstructor: ⇒ SparkDataSource[A])(
    projectFolder: String,
    dataSourceType: String,
    dataSourceName: String,
    processName: String,
    takeSample: Boolean = true,
    persistSample: Boolean = true)
    extends SparkDataSource[A] with Logging {

  CustomTypeConverters.registerTimeConverters()

  lazy val dataSource = sparkDsConstructor

  import RDDPersister._
  import RDDSampler._

  val config = ConfigFactory.load()

  override def apply(): SparkOperation[RDD[A]] = SparkOperation { ctx ⇒
    val (tablePath, fileExists) = cacheFileNameAndStatus(projectFolder, dataSourceType, dataSourceName, processName)
    if (fileExists) {
      log.debug(s"Fetching RDD from file $tablePath")
      val rdd = ctx.objectFile[A](tablePath)
      rdd
    } else {
      val rddTry = Try {
        val rdd = dataSource().run(ctx)
        rdd.first() // force it to read from Cassandra (RDDs are lazy - otherwise recover is never invoked)
        rdd
      }
      val usedSample = if (takeSample) {
        log.info("Taking RDD sample ...")
        sample(rddTry.get, dataSourceType)
      } else rddTry.get

      if (takeSample && persistSample) persistRDD(tablePath, usedSample) else usedSample
    }
  }
}

object SparkTestRDDSource {

  def load[A: ClassTag: RowReaderFactory: ValidRDDType](
    rDDName: String)(implicit projectName: ProjectName): SparkOperation[RDD[A]] =
    SparkOperation { ctx ⇒
      val path = RDDPersister.getPath(projectName.name, rDDName)
      ctx.objectFile[A](path.fullPath)
    }

}
