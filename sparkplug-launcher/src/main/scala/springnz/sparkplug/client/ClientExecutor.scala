package springnz.sparkplug.client

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
  import Coordinator._
  import Constants._

  def create(config: Config = ConfigFactory.load().getConfig(defaultConfigSectionName)) =

    new ClientExecutor {
      val actorSystem = ActorSystem(actorSystemName, config)

      val coordinator: Future[ActorRef] = {
        val readyPromise = Promise[ActorRef]()
        actorSystem.actorOf(Coordinator.props(readyPromise), name = clientActorName)
        readyPromise.future
      }

      override def execute[A](pluginClass: String, data: Option[Any]): Future[A] = {
        val jobRequest = JobRequest(pluginClass, data)
        implicit val ec = actorSystem.dispatcher
        val jobCompletePromise = Promise[Any]()
        coordinator.map { coordinatorActorRef ⇒
          val requestWithPromise = JobRequestWithPromise(jobRequest, Some(jobCompletePromise))
          coordinatorActorRef ! requestWithPromise
        }

        jobCompletePromise.future.map {
          case any ⇒ any.asInstanceOf[A]
        }
      }

      override def shutDown() = {
        implicit val ec = actorSystem.dispatcher
        Await.result(coordinator.map {
          actorRef ⇒
            actorRef ! ShutDown
        }, 20 seconds)
        // give it 20 seconds to try to shut down the Client and Server, then just terminate the actorSystem
        log.info(s"ActorSystem '$actorSystemName' shutting down...")
        actorSystem.shutdown()
      }
    }
}
