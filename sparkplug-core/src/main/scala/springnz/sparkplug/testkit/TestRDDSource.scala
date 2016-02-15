package springnz.sparkplug.testkit

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import springnz.sparkplug.core.SparkOperation

import scala.reflect.ClassTag

object TestRDDSource extends LazyLogging {
  def load[A: ClassTag](
    rDDName: String)(implicit projectName: ProjectName): SparkOperation[RDD[A]] =
    SparkOperation { ctx ⇒
      val path = RDDPersister.getPath(projectName.name, rDDName).pathAsString
      logger.info(s"Loading test data from '$path'")
      ctx.objectFile[A](path)
    }
}

