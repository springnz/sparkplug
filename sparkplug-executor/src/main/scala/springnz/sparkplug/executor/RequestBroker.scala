package springnz.sparkplug.executor

import akka.actor.TypedActor.{ PreStart, PostStop }
import akka.actor._
import better.files._
import springnz.sparkplug.executor.InternalMessageTypes.RoutedRequest
import springnz.sparkplug.executor.MessageTypes.{ ShutDown, ServerReady, JobRequest }
import org.apache.spark.SparkContext
import org.joda.time.DateTime

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

  def receive = receiveJobRequests(0)

  def receiveJobRequests(jobCount: Int): Receive = {
    case job: JobRequest ⇒
      log.info(s"Creating new jobProcessor for '${job.factoryClassName}'.")
      val processor = context.actorOf(JobProcessor.props, s"jobProcessor-$jobCount")
      context.become(receiveJobRequests(jobCount + 1))
      processor ! RoutedRequest(job, sender)

    case ShutDown ⇒
      log.info(s"Shutting down...")
      context.system.shutdown()
  }
}

