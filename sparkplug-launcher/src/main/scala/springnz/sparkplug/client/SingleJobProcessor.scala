package springnz.sparkplug.client

import akka.actor.TypedActor.PreStart
import akka.actor._
import springnz.sparkplug.executor.MessageTypes._
import springnz.util.Logging

import scala.concurrent._

object SingleJobProcessor {
  def props(broker: ActorRef, requestor: ActorRef, jobRequest: JobRequest, promise: Option[Promise[Any]], jobIndex: Int) =
    Props(new SingleJobProcessor(broker, requestor, jobRequest, promise, jobIndex))
}

class SingleJobProcessor(broker: ActorRef, requestor: ActorRef, jobRequest: JobRequest, promise: Option[Promise[Any]], jobIndex: Int)
    extends Actor with PreStart with Logging {

  import Coordinator._

  override def preStart() = {
    log.info(s"SingleJobProcessor delegated to run request: $jobRequest")
    broker ! jobRequest
  }

  override def receive: Receive = {
    case jobSuccess @ JobSuccess(jobRequest, result) ⇒
      log.info(s"Received Result from sender: $sender")
      log.info(s"Result value: $result")
      // tell the requestor the job succeeded, but make it look like it came from the coordinator
      requestor.tell(jobSuccess, context.parent)
      promise.foreach(_.success(result))
      context.parent ! JobCompleteIndex(jobIndex)
      self ! PoisonPill

    case jobFailure @ JobFailure(jobRequest, reason) ⇒
      log.info(s"Received JobFailure from sender: $sender")
      log.info(s"Reason: ${reason.getMessage}")
      requestor.tell(jobFailure, context.parent)
      promise.foreach(_.failure(reason))
      context.parent ! JobCompleteIndex(jobIndex)
      self ! PoisonPill
  }

}

