package springnz.sparkplug.executor

import akka.actor.ActorRef
import springnz.sparkplug.executor.MessageTypes.JobRequest
import springnz.sparkplug.core.SparkOperation

object MessageTypes {
  case object ClientReady

  case object ServerReady
  case object ShutDown
  case object CancelAllJobs

  case class ServerError(reason: Throwable)
  case class JobRequest(factoryClassName: String, data: Option[Any] = None)

  case class JobSuccess(jobRequest: JobRequest, response: Any)
  case class JobFailure(jobRequest: JobRequest, reason: Throwable)

  class SparkplugException(message: String) extends Exception(message)
}

object InternalMessageTypes {
  case class RoutedRequest(jobRequest: JobRequest, originator: ActorRef)
}
