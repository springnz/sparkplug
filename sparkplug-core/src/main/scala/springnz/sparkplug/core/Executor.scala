package springnz.sparkplug.core

import org.apache.spark.SparkContext
import springnz.util.Logging
import springnz.util.Pimpers._

import scala.util.Try

trait ExecutorU {
  protected def configurer: Configurer
  def execute[A](operation: SparkOperation[A]): Any
}

trait Executor[B] extends ExecutorU {
  def execute[A](operation: SparkOperation[A]): B
}

// A SparkExecutor creates a spark context on the fly and releases it
trait SparkExecutor extends Executor[Try[_]] with Logging {
  def execute[A](operation: SparkOperation[A]): Try[A] = {
    log.debug(s"SparkExecutor configuration:\n$configurer")
    configurer { cfg ⇒
      val ctx = new SparkContext(cfg)
      Try {
        log.debug("Executing operation: " + operation.getClass.getName)
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
trait LongLivedExecutor extends Executor[Try[_]] with Logging {

  lazy val sparkContext: SparkContext = configurer { cfg ⇒ new SparkContext(cfg) }

  def execute[A](operation: SparkOperation[A]): Try[A] =
    Try {
      log.debug("Executing operation: " + operation.getClass.getName)
      operation.run(sparkContext)
    }.withErrorLog("Error executing operation")

  def stopContext(): Unit = sparkContext.stop()

}

