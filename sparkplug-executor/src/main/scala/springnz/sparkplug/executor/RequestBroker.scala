package springnz.sparkplug.executor

import akka.actor.TypedActor.{ PostStop, PreStart }
import akka.actor._
import org.apache.spark.SparkContext
import springnz.sparkplug.core.SparkPlugException
import springnz.sparkplug.executor.InternalMessageTypes.RoutedRequest
import springnz.sparkplug.executor.MessageTypes._

import scala.util.Try

class RequestBroker(sparkClient: String, postStopAction: ⇒ Unit)(implicit sparkContext: SparkContext)
    extends Actor with PreStart with PostStop with ActorLogging {

  override def preStart() = {
    Try {
      log.info(s"RequestBroker preStart called. Logging Spark Configuration below:")
      // logging Spark Configuration
      val sparkConfDebug = sparkContext.getConf
        .toDebugString
        .lines
        .filterNot(_.contains("password"))
        .mkString("\n")
      log.info(s"Spark-Configuration:\n$sparkConfDebug\n")

      log.info(s"Notifying client at address '$sparkClient' of readiness.")
      if (sparkClient.nonEmpty) {
        log.info(s"Trying to contact sparkClient at $sparkClient")
        val clientActor = context.actorSelection(sparkClient)
        log.info(s"Sending ServerReady to contact sparkClient at $sparkClient")
        clientActor ! ServerReady
      }
      log.info("Finished loading RequestBroker. Ready for action.")
    }.recover {
      case reason ⇒
        log.error(s"Error initialising Request Broker.", reason)
        log.error(s"Sending the ShutDown message.")
        self ! ShutDown
    }
  }

  override def postStop() = {
    log.info("Running postStopAction")
    postStopAction
  }

  type JobRequestInfo = (ActorRef, JobRequest)

  def receive = receiveJobRequests(0, None, Map.empty[String, JobRequestInfo])

  def receiveJobRequests(jobCount: Int, clientRef: Option[ActorRef], processorMap: Map[String, JobRequestInfo]): Receive = {
    case job: JobRequest ⇒
      log.info(s"Creating new jobProcessor for '${job.factoryClassName}'.")
      val processor = context.actorOf(JobProcessor.props, s"jobProcessor-$jobCount")
      context.become(receiveJobRequests(jobCount + 1, clientRef, processorMap.updated(processor.path.toString, (sender, job))))
      context.watch(processor)
      processor ! RoutedRequest(job, sender)

    case ShutDown ⇒
      log.info(s"Shutting down...")
      context.system.shutdown()

    case ClientReady ⇒
      log.info("Received ClientReady message from Client. Deathwatching on client.")
      context.become(receiveJobRequests(jobCount, Some(sender), processorMap))
      context.watch(sender)

    case Terminated(terminatedRef) ⇒
      clientRef.foreach { clientRefInner ⇒
        if (terminatedRef.path.toString == clientRefInner.path.toString) {
          log.info("Client was terminated (or timed out). Sending the shutdown signal.")
          self ! ShutDown
        }
      }
      processorMap.get(terminatedRef.path.toString).foreach {
        case (requestor, job) ⇒
          val message = s"Processor '${self.path.toString}' terminated (or timed out) for job $job."
          log.error(message)
          requestor ! JobFailure(job, new SparkPlugException(message))
      }

    case CancelAllJobs ⇒
      log.info(s"Cancelling all Spark Jobs...")
      sparkContext.cancelAllJobs()
  }
}

