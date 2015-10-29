package springnz.sparkplug.client

import akka.actor.TypedActor.PreStart
import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import springnz.sparkplug.executor.MessageTypes._
import springnz.util.{ Pimpers, Logging }

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try

trait ClientExecutor {
  def execute[A](pluginClass: String, data: Option[Any]): Future[A]
  def shutDown(): Unit
}

object ClientExecutor extends Logging {
  import Constants._
  import Coordinator._
  import Pimpers._

  def create(config: Config = ConfigFactory.load().getConfig(defaultConfigSectionName)) = {

    val actorSystem: ActorSystem = {
      Try { ActorSystem(actorSystemName, config) }
        .withErrorLog("Unable to create ActorSystem")
        .get
    }

    implicit val executionContext = actorSystem.dispatcher

    // Create a parent actor to receive messages (so they don't get sent to deadLetters)
    // This actor then creates the Coordinator
    val coordinatorFuture: Future[ActorRef] = {
      val readyPromise = Promise[ActorRef]()
      actorSystem.actorOf(Coordinator.props(readyPromise), name = coordinatorActorName)
      readyPromise.future
    }

    new ClientExecutor {

      override def execute[A](pluginClass: String, data: Option[Any]): Future[A] = {
        val jobRequest = JobRequest(pluginClass, data)
        val jobCompletePromise = Promise[Any]()
        coordinatorFuture.map {
          coordinator ⇒
            val requestWithPromise = JobRequestWithPromise(jobRequest, Some(jobCompletePromise))
            coordinator ! requestWithPromise
        }

        // Complete the jobCompletePromise straight away if initialisation fails:
        coordinatorFuture.onFailure {
          case reason ⇒
            log.info(s"Future failed to complete request $jobRequest. Error: ${reason.getMessage}")
            jobCompletePromise.failure(reason)
        }
        jobCompletePromise.future.map {
          case any ⇒
            log.info(s"Future complete request $jobRequest. Return value: $any")
            any.asInstanceOf[A]
        }
      }

      override def shutDown() = {
        Await.result(coordinatorFuture.map {
          coordinator ⇒ coordinator ! ShutDown // to release the remote connection
        }, 20 seconds)
        // give it 20 seconds to try to shut down the Client and Server, then just terminate the actorSystem
        log.info(s"ActorSystem '$actorSystemName' shutting down...")
        actorSystem.shutdown()
      }
    }
  }
}
