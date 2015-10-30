package springnz.sparkplug.executor

import akka.actor._
import springnz.sparkplug.executor.InternalMessageTypes.RoutedRequest
import springnz.sparkplug.executor.MessageTypes.{ JobFailure, JobRequest, JobSuccess }
import springnz.sparkplug.core.SparkPlugin
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

  def executeJob(job: JobRequest, originator: ActorRef) = {
    val factoryName = job.factoryClassName

    implicit val ec = context.dispatcher
    val f: Future[Any] = Future {
      log.info(s"Loading and instantiating job '$factoryName'.")
      val factory = Class.forName(factoryName).newInstance().asInstanceOf[SparkPlugin]
      val operation = factory.apply(job.data)
      log.info(s"Executing job '$factoryName'.")
      val result = operation.run(sparkContext)
      log.info(s"Job '$factoryName' finished.")

      val listener = new SparkListener {}
      sparkContext.addSparkListener(listener)

      result
    }
    f.onComplete {
      case Success(result) ⇒ originator ! JobSuccess(job, result)
      case Failure(e)      ⇒ originator ! JobFailure(job, e)
    }
  }

  def receive = {
    case RoutedRequest(job, originator: ActorRef) ⇒ executeJob(job, originator)
  }
}

