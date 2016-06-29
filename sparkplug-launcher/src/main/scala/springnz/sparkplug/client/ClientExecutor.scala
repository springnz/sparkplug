package springnz.sparkplug.client

import akka.actor._
import com.typesafe.config.{ ConfigFactory, Config }
import com.typesafe.scalalogging.LazyLogging
import springnz.sparkplug.executor.MessageTypes._
import springnz.sparkplug.util.Pimpers

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try

trait ClientExecutor {
  def execute[A](pluginClass: String, data: Option[Any]): Future[A]
  def cancelJobs(): Unit
  def shutDown(): Unit
}

object ClientExecutor extends LazyLogging {
  import Constants._
  import Coordinator._
  import Pimpers._
  implicit lazy val clientLogger = logger

  case class ClientExecutorParams(
    akkaClientConfig: Config = defaultClientAkkaConfig,
    sparkConfig: Config = defaultSparkConfig,
    akkaRemoteConfig: Option[Config] = None,
    jarPath: Option[String] = None,
    cancelJobsTimeout: Duration = 10.seconds,
    shutdownTimeout: Duration = 15.seconds)

  def apply[A](pluginClass: String, data: Option[Any], params: ClientExecutorParams = ClientExecutorParams())(implicit ec: ExecutionContext): Future[A] = {
    val executor = create(params)
    executor
      .execute(pluginClass, data)
      .andThen { case _ ⇒ executor.shutDown() }
  }

  def create(params: ClientExecutorParams = ClientExecutorParams()): ClientExecutor = {

    val actorSystem: ActorSystem = {
      Try { ActorSystem(actorSystemName, params.akkaClientConfig) }
        .withErrorLog("Unable to create ActorSystem")
        .get
    }

    implicit val executionContext = actorSystem.dispatcher

    // Create a parent actor to receive messages (so they don't get sent to deadLetters)
    // This actor then creates the Coordinator
    val coordinatorFuture: Future[ActorRef] = {
      val readyPromise = Promise[ActorRef]()
      actorSystem.actorOf(
        Coordinator.props(
          Some(readyPromise),
          akkaClientConfig = params.akkaClientConfig,
          sparkConfig = params.sparkConfig,
          akkaRemoteConfig = params.akkaRemoteConfig,
          jarPath = params.jarPath),
        name = coordinatorActorName)
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
            logger.info(s"Future failed to complete request $jobRequest. Error: ${reason.getMessage}")
            jobCompletePromise.failure(reason)
        }
        jobCompletePromise.future.map {
          case any ⇒
            logger.info(s"Future complete request $jobRequest. Return value: $any")
            any.asInstanceOf[A]
        }
      }

      override def cancelJobs(): Unit = {
        coordinatorFuture.foreach(coordinator ⇒ coordinator ! CancelAllJobs)
        Await.result(coordinatorFuture, params.cancelJobsTimeout)
      }

      override def shutDown(): Unit = {
        coordinatorFuture.foreach(coordinator ⇒ coordinator ! ShutDown)
        try {
          Await.result(coordinatorFuture, params.shutdownTimeout)
        } finally {
          // give it x seconds to try to shut down the Client and Server, then just terminate the actorSystem
          logger.info(s"ActorSystem '$actorSystemName' shutting down...")
          actorSystem.shutdown()
          actorSystem.awaitTermination(params.shutdownTimeout)
        }
      }
    }
  }

  lazy val defaultConfig = ConfigFactory.load.getConfig("sparkplug")

  def defaultClientAkkaConfig: Config = defaultConfig.getConfig(defaultAkkaConfigSection)
  def defaultSparkConfig: Config = defaultConfig.getConfig(defaultSparkConfigSection)

}
