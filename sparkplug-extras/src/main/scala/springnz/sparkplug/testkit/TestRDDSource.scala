package springnz.sparkplug.testkit

import org.apache.spark.rdd.RDD
import springnz.sparkplug.core.SparkOperation
import springnz.util.Logging

import scala.reflect.ClassTag

object TestRDDSource extends Logging {
  def load[A: ClassTag](
    rDDName: String)(implicit projectName: ProjectName): SparkOperation[RDD[A]] =
    SparkOperation { ctx ⇒
      val path = RDDPersister.getPath(projectName.name, rDDName).fullPath
      log.info(s"Loading test data from '$path'")
      ctx.objectFile[A](path)
    }
}

