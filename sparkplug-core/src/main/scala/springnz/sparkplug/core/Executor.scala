package springnz.sparkplug.core

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import springnz.sparkplug.util.Pimpers._

import scala.util.Try

trait ExecutorU {
  protected def configurer: Configurer
  def execute[A](operation: SparkOperation[A]): Any
}

trait Executor[B] extends ExecutorU {
  def execute[A](operation: SparkOperation[A]): B
}

// A SparkExecutor creates a spark context on the fly and releases it
trait SparkExecutor extends Executor[Try[_]] with LazyLogging {
  def execute[A](operation: SparkOperation[A]): Try[A] = {
    logger.debug(s"SparkExecutor configuration:\n$configurer")
    configurer { cfg ⇒
      val ctx = new SparkContext(cfg)
      implicit lazy val log = logger
      Try {
        logger.debug("Executing operation: " + operation.getClass.getName)
        operation.run(ctx)
      }.withErrorLog("Error executing operation")
        .withFinally {
          ctx.stop()
        }
    }
  }
}

// A LongLivedExecutor creates a spark context and keeps it for as long as it lives
// The StopContext must be manually called (just like in Spark)
trait LongLivedExecutor extends Executor[Try[_]] with LazyLogging {

  lazy val sparkContext: SparkContext = configurer { cfg ⇒ new SparkContext(cfg) }

  def execute[A](operation: SparkOperation[A]): Try[A] = {
    implicit lazy val log = logger
    Try {
      logger.debug("Executing operation: " + operation.getClass.getName)
      operation.run(sparkContext)
    }.withErrorLog("Error executing operation")
  }

  def stopContext(): Unit = sparkContext.stop()

}

