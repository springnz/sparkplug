package springnz.sparkplug.testkit

import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import springnz.sparkplug.core.SparkOperation

import scala.reflect.ClassTag

object TestExtensions {

  private def persistTestResource[A: ClassTag](rdd: RDD[A], rddName: String, overwrite: Boolean = false)(
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

  implicit class SparkRDDOps[A: ClassTag](operation: SparkOperation[RDD[A]]) {
    import RDDSamplers._

    def saveTo(rddName: String, sampler: RDD[A] ⇒ RDD[A] = identitySampler)(
      implicit projectName: ProjectName, log: Logger): SparkOperation[RDD[A]] =
      operation.map {
        rdd ⇒
          val sampled = sampler(rdd)
          persistTestResource(sampled, rddName, overwrite = false)
          sampled
      }

    def sourceFrom(rddName: String, sampler: RDD[A] ⇒ RDD[A] = identitySampler)(
      implicit projectName: ProjectName, log: Logger): SparkOperation[RDD[A]] =
      SparkOperation { ctx ⇒
        val path = RDDPersister.getPath(projectName.name, rddName)
        if (path.exists)
          ctx.objectFile[A](path.fullPath)
        else {
          val rdd = operation.run(ctx)
          val sampled = sampler(rdd)
          persistTestResource(sampled, rddName, overwrite = false)
          sampled
        }
      }
  }

  implicit class SparkDFOps(operation: SparkOperation[DataFrame]) {
    import RDDSamplers._

    def saveTo(rddName: String,
      overwrite: Boolean = false,
      sampler: RDD[String] ⇒ RDD[String] = identitySampler)(
        implicit projectName: ProjectName, log: Logger): SparkOperation[DataFrame] =
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
          ctx.objectFile[String](path.fullPath)
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

  // Makes a count method available on a SparkOperation that returns a RDD
  implicit class CountConverter[A: ClassTag](operation: SparkOperation[RDD[A]])(implicit log: Logger) {
    def count = {
      operation.map { rdd ⇒
        val n = rdd.count
        log.debug(s"Produced an RDD of size ${n}")
        n
      }
    }

    def takeWithCount(n: Int) = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        (rdd.take(n), count)
      }
    }

    def collectWithCount() = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        (rdd.collect, count)
      }
    }

    def takeOrderedWithCount(n: Int)(implicit ord: Ordering[A]) = {
      operation.map { rdd ⇒
        val count = rdd.count
        log.debug(s"Produced an RDD of size $count")
        (rdd.takeOrdered(n), count)
      }
    }

  }

}
