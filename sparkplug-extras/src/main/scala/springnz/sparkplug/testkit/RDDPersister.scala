package springnz.sparkplug.testkit

import better.files._
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import springnz.util.Logging
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RDDPersister extends Logging {

  def cacheFileNameAndStatus[A: ClassTag: RowReaderFactory: ValidRDDType](
    projectFolder: String, dataSourceType: String, dataSourceName: String, processName: String): (String, Boolean) = {

    val userDir = root / System.getProperty("user.dir")
    if (!(userDir / "build.sbt").exists)
      throw new Exception("The test should be run from the base folder")

    val testFolder = userDir / projectFolder / "src" / "test" / "resources" / "testdata" / dataSourceType / dataSourceName
    val classTagName = implicitly[ClassTag[A]].toString.replace('$', '.')
    val tablePath = testFolder / s"$processName-$classTagName.tbl"
    val fileExists = tablePath.exists
    (tablePath.fullPath, fileExists)
  }

  def persistSampledRDD[A: ClassTag: RowReaderFactory: ValidRDDType](
    tablePath: String, rdd: RDD[A], rddDestPartitions: Int = 10): RDD[A] = {
    log.debug(s"Saving RDD to $tablePath ...")
    // reduce the number of partitions
    val repartitionedRdd = if (rdd.partitions.length <= rddDestPartitions) rdd else rdd.repartition(rddDestPartitions)
    repartitionedRdd.saveAsObjectFile(tablePath)
    repartitionedRdd
  }

}
