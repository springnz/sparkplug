package springnz.sparkplug.testkit

import better.files._
import org.apache.spark.rdd.RDD
import springnz.sparkplug.util.Logging

object RDDPersister extends Logging {

  def persistRDD[A](
    tablePath: String, rdd: RDD[A], rddDestPartitions: Int = 10): RDD[A] = {
    log.debug(s"Saving RDD to $tablePath ...")
    // reduce the number of partitions
    val repartitionedRdd = if (rdd.partitions.length <= rddDestPartitions) rdd else rdd.repartition(rddDestPartitions)
    repartitionedRdd.saveAsObjectFile(tablePath)
    repartitionedRdd
  }

  def getPath(projectName: String, rddName: String): File =
    projectName / "src" / "test" / "resources" / "testdata" / rddName

  def checkIfPersistedRDDExists(projectName: String, rddName: String) =
    getPath(projectName, rddName).exists

}

