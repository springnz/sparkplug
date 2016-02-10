package springnz.sparkplug.executor

import akka.actor.ActorRef
import springnz.sparkplug.core.{ SparkPluginI, SparkPlugin }
import springnz.sparkplug.executor.MessageTypes.JobRequest

object MessageTypes {
  case object ClientReady

  case object ServerReady
  case object ShutDown

  case class ServerError(reason: Throwable)
  case class JobRequest(plugin: () ⇒ SparkPluginI, data: Option[Any] = None)

  case class JobSuccess(jobRequest: JobRequest, response: Any)
  case class JobFailure(jobRequest: JobRequest, reason: Throwable)

}

object InternalMessageTypes {
  case class RoutedRequest(jobRequest: JobRequest, originator: ActorRef)
}
