package springnz.sparkplug.testkit

import com.typesafe.scalalogging.{ LazyLogging, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.util.Logging

import scala.reflect.ClassTag

object TestExtensions extends LazyLogging {

  private def persistTestResource[A: ClassTag](rdd: RDD[A], rddName: String, overwrite: Boolean = false)(
    implicit projectName: ProjectName): RDD[A] = {
    val path = RDDPersister.getPath(projectName.name, rddName)
    if (overwrite || (!overwrite && !path.exists)) {
      if (path.exists) {
        logger.info(s"deleting existing RDD at ${path.pathAsString}")
        path.delete()
      }
      RDDPersister.persistRDD(path.pathAsString, rdd)
    } else { // (!overwrite && path.exists)
      logger.info(s"Not persisting RDD that already exists at path [${path.pathAsString}]")
      rdd
    }
  }

  class SparkRDDOps[A: ClassTag](operation: SparkOperation[RDD[A]]) {
    import RDDSamplers._

    def saveTo(rddName: String, sampler: RDD[A] ⇒ RDD[A] = identitySampler)(
      implicit projectName: ProjectName): SparkOperation[RDD[A]] =
      operation.map {
        rdd ⇒
          val sampled = sampler(rdd)
          persistTestResource(sampled, rddName, overwrite = false)
          sampled
      }

    def sourceFrom(rddName: String, sampler: RDD[A] ⇒ RDD[A] = identitySampler)(
      implicit projectName: ProjectName): SparkOperation[RDD[A]] =
      SparkOperation { ctx ⇒
        val path = RDDPersister.getPath(projectName.name, rddName)
        if (path.exists)
          ctx.objectFile[A](path.pathAsString)
        else {
          val rdd = operation.run(ctx)
          val sampled = sampler(rdd)
          persistTestResource(sampled, rddName, overwrite = false)
          sampled
        }
      }
  }

  class SparkDFOps(operation: SparkOperation[DataFrame]) {
    import RDDSamplers._

    def saveTo(rddName: String,
      overwrite: Boolean = false,
      sampler: RDD[String] ⇒ RDD[String] = identitySampler)(
        implicit projectName: ProjectName): SparkOperation[DataFrame] =
      operation.map {
        df ⇒
          val rdd: RDD[String] = df.toJSON
          val sampled = sampler(rdd)
          persistTestResource(sampled, rddName, overwrite)
          val sqlContext = new SQLContext(sampled.sparkContext)
          sqlContext.read.json(sampled)
      }

    def sourceFrom(dataFrameName: String,
      overwrite: Boolean = false,
      sampler: RDD[String] ⇒ RDD[String] = rdd ⇒ rdd)(
        implicit projectName: ProjectName, log: Logger): SparkOperation[DataFrame] =
      SparkOperation { ctx ⇒
        val path = RDDPersister.getPath(projectName.name, dataFrameName)
        val sampledRDD = if (path.exists)
          ctx.objectFile[String](path.pathAsString)
        else {
          val df = operation.run(ctx)
          val rdd: RDD[String] = df.toJSON
          val sampled = sampler(rdd)
          persistTestResource(sampled, dataFrameName, overwrite)
          sampled
        }
        val sqlContext = new SQLContext(ctx)
        sqlContext.read.json(sampledRDD)
      }

  }
}
