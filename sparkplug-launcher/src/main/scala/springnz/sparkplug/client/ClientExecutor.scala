package springnz.sparkplug.client

import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import springnz.sparkplug.executor.MessageTypes._
import springnz.sparkplug.util.{ Pimpers, Logging }

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

  case class ClientExecutorParams(config: Config = defaultConfig(), jarPath: Option[String] = None)

  def defaultConfig() = ConfigFactory.load().getConfig(defaultConfigSectionName)

  def apply[A](pluginClass: String, data: Option[Any], params: ClientExecutorParams = ClientExecutorParams())(implicit ec: ExecutionContext): Future[A] = {
    val executor = create(params)
    executor
      .execute(pluginClass, data)
      .andThen { case _ ⇒ executor.shutDown() }
  }

  def create(params: ClientExecutorParams = ClientExecutorParams()): ClientExecutor = {

    val actorSystem: ActorSystem = {
      Try { ActorSystem(actorSystemName, params.config) }
        .withErrorLog("Unable to create ActorSystem")
        .get
    }

    implicit val executionContext = actorSystem.dispatcher

    // Create a parent actor to receive messages (so they don't get sent to deadLetters)
    // This actor then creates the Coordinator
    val coordinatorFuture: Future[ActorRef] = {
      val readyPromise = Promise[ActorRef]()
      actorSystem.actorOf(Coordinator.props(Some(readyPromise), params.config, params.jarPath), name = coordinatorActorName)
      readyPromise.future
    }

    new ClientExecutor {

      override def execute[A](pluginClass: String, data: Option[Any]): Future[A] = {
        val jobRequest = JobRequest(pluginClass, data)
        val jobCompletePromise = Promise[Any]()
        coordinatorFuture.foreach {
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

      override def shutDown(): Unit = {
        coordinatorFuture.foreach(coordinator ⇒ coordinator ! ShutDown)
        Await.result(coordinatorFuture, 10.seconds)
        // give it 20 seconds to try to shut down the Client and Server, then just terminate the actorSystem
        log.info(s"ActorSystem '$actorSystemName' shutting down...")
        actorSystem.shutdown()
      }
    }
  }
}
