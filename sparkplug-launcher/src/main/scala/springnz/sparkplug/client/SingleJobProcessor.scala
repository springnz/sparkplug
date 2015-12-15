package springnz.sparkplug.client

import akka.actor.TypedActor.PreStart
import akka.actor._
import springnz.sparkplug.core.SparkPlugException
import springnz.sparkplug.executor.MessageTypes._
import springnz.util.Logging

import scala.concurrent._

object SingleJobProcessor {
  def props(jobRequest: JobRequest, broker: ActorRef, requestor: Option[ActorRef], promise: Option[Promise[Any]], jobIndex: Int) =
    Props(new SingleJobProcessor(jobRequest, broker, requestor, promise, jobIndex))
}

class SingleJobProcessor(jobRequest: JobRequest, broker: ActorRef, requestor: Option[ActorRef], promise: Option[Promise[Any]], jobIndex: Int)
    extends Actor with PreStart with Logging {

  import Coordinator._

  override def preStart() = {
    log.info(s"SingleJobProcessor delegated to run request: $jobRequest")
    context.watch(broker)
    broker ! jobRequest
  }

  override def receive: Receive = {
    case jobSuccess @ JobSuccess(_, result) ⇒
      log.info(s"Received Result from sender: $sender")
      log.info(s"Result value: $result")
      // tell the requestor the job succeeded, but make it look like it came from the coordinator
      val coordinator = context.parent
      requestor.foreach(_.tell(jobSuccess, coordinator))
      promise.foreach(_.success(result))
      coordinator ! JobCompleteIndex(jobIndex)
      self ! PoisonPill

    case jobFailure @ JobFailure(_, reason) ⇒
      log.error(s"Received JobFailure from sender: $sender")
      log.error(s"Reason: ${reason.getMessage}")
      val coordinator = context.parent
      requestor.foreach(_.tell(jobFailure, coordinator))
      promise.foreach(_.failure(reason))
      coordinator ! JobCompleteIndex(jobIndex)
      self ! PoisonPill

    case Terminated(_) ⇒
      val msg = s"Lost contact with broker, or broker terminated: $sender"
      log.error(msg)
      self ! JobFailure(jobRequest, new SparkPlugException(msg))
  }
}

