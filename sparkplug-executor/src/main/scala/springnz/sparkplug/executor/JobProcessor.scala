package springnz.sparkplug.executor

import akka.actor._
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListener
import springnz.sparkplug.core.SparkPlugin
import springnz.sparkplug.executor.InternalMessageTypes.RoutedRequest
import springnz.sparkplug.executor.MessageTypes.{ JobFailure, JobRequest, JobSuccess }
import springnz.sparkplug.util.Logging

import scala.concurrent.Future
import scala.util._

object JobProcessor {
  // Uses it's own dispatcher for bulkheading
  def props(implicit sparkContext: SparkContext): Props = Props(new JobProcessor).withDispatcher("akka.sparkplug-job-dispatcher")
}

class JobProcessor(implicit sparkContext: SparkContext) extends Actor with Logging {
  def executeJob(job: JobRequest, originator: ActorRef) = {
    implicit val ec = context.dispatcher
    val f: Future[Any] = Future {
      log.info(s"Loading and instantiating job from factory.")
      val plugin = job.factory()
      val operation = plugin.apply()

      log.info(s"Executing job '$plugin'.")
      val result = operation.run(sparkContext)
      log.info(s"Job '$plugin' finished.")

      // TODO: do something about the SparkListener (not sure what)
      val listener = new SparkListener {}
      sparkContext.addSparkListener(listener)

      result
    }
    f.onComplete {
      case Success(result) ⇒
        log.info(s"Successfully processed job $job. Notifying client.")
        log.debug(s"Job result: $result")
        originator ! JobSuccess(job, result)

      case Failure(reason) ⇒
        log.error(s"Job processing of job $job failed. Notifying client", reason)
        originator ! JobFailure(job, reason)
    }
  }

  def receive = {
    case RoutedRequest(job, originator: ActorRef) ⇒
      log.info(s"JobProcessor ${self.path.toString} routed request to process job ${job}")
      executeJob(job, originator)
  }
}

