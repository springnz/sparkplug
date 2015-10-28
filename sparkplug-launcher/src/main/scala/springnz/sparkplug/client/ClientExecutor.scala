package springnz.sparkplug.client

import akka.actor.TypedActor.PreStart
import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import springnz.sparkplug.executor.MessageTypes._
import springnz.util.Logging

import scala.concurrent._
import scala.concurrent.duration._

trait ClientExecutor {
  def execute[A](pluginClass: String, data: Option[Any]): Future[A]
  def shutDown(): Unit
}

object ClientExecutor extends Logging {
  import Constants._
  import Coordinator._

  def create(config: Config = ConfigFactory.load().getConfig(defaultConfigSectionName)) = {

    case class ClientActors(parent: ActorRef, coordinator: ActorRef)

    val actorSystem = ActorSystem(actorSystemName, config)
    implicit val executionContext = actorSystem.dispatcher

    // Create a parent actor to receive messages (so they don't get sent to deadLetters)
    // This actor then creates the Coordinator
    val actors: Future[ClientActors] = {
      val readyPromise = Promise[ActorRef]()

      val parentActor = actorSystem.actorOf(Props(new Actor with Logging with PreStart {
        override def preStart() = context.actorOf(Coordinator.props(readyPromise), name = coordinatorActorName)

        override def receive: Receive = {
          case ServerError(reason) ⇒ {
            log.error(s"Top level actor received a server error: ${reason.toString}")
          }
          case msg ⇒ log.info(s"Top level actor received message: $msg")
        }
      }), executorActorName)

      Future { parentActor } zip readyPromise.future map {
        case (parent, coordinator) ⇒ ClientActors(parent, coordinator)
      }
    }

    new ClientExecutor {

      override def execute[A](pluginClass: String, data: Option[Any]): Future[A] = {
        val jobRequest = JobRequest(pluginClass, data)
        val jobCompletePromise = Promise[Any]()
        actors.map {
          case ClientActors(parent, coordinator) ⇒
            val requestWithPromise = JobRequestWithPromise(jobRequest, Some(jobCompletePromise))
            coordinator.tell(requestWithPromise, parent)
        }

        // Complete the jobCompletePromise straight away if initialisation fails:
        actors.onFailure {
          case reason ⇒ jobCompletePromise.failure(reason)
        }
        jobCompletePromise.future.map {
          case any ⇒ any.asInstanceOf[A]
        }
      }

      override def shutDown() = {
        Await.result(actors.map {
          case ClientActors(parent, coordinator) ⇒
            coordinator ! ShutDown // to release the remote connection
        }, 20 seconds)
        // give it 20 seconds to try to shut down the Client and Server, then just terminate the actorSystem
        log.info(s"ActorSystem '$actorSystemName' shutting down...")
        actorSystem.shutdown()
      }
    }
  }
}
