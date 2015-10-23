package springnz.sparkplug.testkit

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import springnz.sparkplug.core.{ SparkOperation, SparkProcess }
import springnz.util.Logging

class PersistableDFProcess(projectFolder: String, processName: String, mustSample: Boolean = true)(dfProcess: SparkProcess[DataFrame])
    extends SparkProcess[DataFrame] with Logging {

  import RDDPersister._
  import RDDSampler._
  val dataSourceType = "dataframe"

  val (tablePath, fileExists) = cacheFileNameAndStatus(projectFolder, dataSourceType, processName, dfProcess.getClass.getName)

  // TODO: refactor to provide native support for DataFrames, rather than go via RDDs
  override def apply() = SparkOperation { ctx ⇒
    val sqlContext = new SQLContext(ctx)
    val sampledRDD = if (fileExists) {
      val rdd = ctx.objectFile[String](tablePath)
      log.debug(s"Fetched RDD from $tablePath")
      rdd
    } else {
      def operation = dfProcess.apply()
      val df = operation.run(ctx)
      val rdd: RDD[String] = df.toJSON
      val sampledRdd = if (mustSample) sample(rdd, dataSourceType) else rdd
      persistSampledRDD(tablePath, sampledRdd)
    }
    sqlContext.read.json(sampledRDD)
  }
}
