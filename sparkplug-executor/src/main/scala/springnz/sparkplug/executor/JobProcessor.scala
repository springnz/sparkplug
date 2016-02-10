package springnz.sparkplug.executor

import java.util.ServiceLoader

import akka.actor._
import springnz.sparkplug.executor.InternalMessageTypes.RoutedRequest
import springnz.sparkplug.executor.MessageTypes.{ JobFailure, JobRequest, JobSuccess }
import springnz.sparkplug.core.{ Descriptor, SparkOperation, SparkPlugin }
import springnz.util.{ Logging, Pimpers }
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListener

import scala.concurrent.Future
import scala.util._

object JobProcessor {
  // Uses it's own dispatcher for bulkheading
  def props(implicit sparkContext: SparkContext): Props = Props(new JobProcessor).withDispatcher("akka.sparkplug-job-dispatcher")
}

class JobProcessor(implicit sparkContext: SparkContext) extends Actor with Logging {

  def executeJob[A, R](job: JobRequest, originator: ActorRef): Unit = {
    val descriptor = job.descriptor
    val factoryName = descriptor.classname

    implicit val ec = context.dispatcher
    val f: Future[descriptor.R] = Future {
      log.info(s"Loading and instantiating job '$factoryName'.")
      val factoryAny = Class.forName(factoryName).newInstance()
      val sp = factoryAny.asInstanceOf[SparkPlugin with Descriptor]
      val operation = sp.apply(job.data.asInstanceOf[Option[sp.A]])

      log.info(s"Executing job '$factoryName'.")
      val result = operation.run(sparkContext)
      log.info(s"Job '$factoryName' finished.")

      // TODO: do something about the SparkListener (not sure what)
      val listener = new SparkListener {}
      sparkContext.addSparkListener(listener)

      result.asInstanceOf[descriptor.R]
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

