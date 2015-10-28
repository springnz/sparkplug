package springnz.sparkplug.client

import akka.actor.TypedActor.PreStart
import akka.actor._
import com.typesafe.config.ConfigFactory
import springnz.sparkplug.client.Coordinator.JobRequestWithPromise
import springnz.sparkplug.executor.MessageTypes._
import springnz.util.Logging

import scala.concurrent._
import scala.util.{ Try, Success }

object Coordinator {
  case class JobRequestWithPromise(jobRequest: JobRequest, promise: Option[Promise[Any]])
  case class JobCompleteIndex(jobIndex: Int)

  def props(readyPromise: Promise[ActorRef]) = Props(new Coordinator(readyPromise))
}

class Coordinator(readyPromise: Promise[ActorRef]) extends Actor with PreStart with Logging {
  import Coordinator._
  import Constants._

  override def preStart() = {
    val launchTry: Try[Future[Unit]] = Try {
      val config = ConfigFactory.load()
      val hostName = config.getString(s"$defaultConfigSectionName.akka.remote.netty.tcp.hostname")
      val port = config.getInt(s"$defaultConfigSectionName.akka.remote.netty.tcp.port")
      val clientPath = context.self.path.toString
      val localPath = clientPath.substring(clientPath.indexOf("/user/"))
      val clientAkkaAddress = s"akka.tcp://$actorSystemName@$hostName:$port$localPath"
      Launcher.launch(clientAkkaAddress, jarPath, mainJar, mainClass).get
    }
    if (launchTry.isFailure) {
      val reason = launchTry.failed.get
      log.error(s"Error was caught with in Coordinator preStart: ${reason.toString}")
      context.parent ! ServerError(reason)
      readyPromise.failure(reason)
      self ! PoisonPill
    }
  }

  override def receive: Receive = {
    case ServerReady ⇒
      val broker = sender
      context.become(waitForRequests(broker, 0, Set.empty[Int]))
      log.info(s"Coordinator received a ServerReady message from broker: ${broker.path.toString}")
      context.parent ! ServerReady
      readyPromise.complete(Success(self))
  }

  def waitForRequests(broker: ActorRef, jobCounter: Int, jobsOutstanding: Set[Int]): Receive = {

    case request: JobRequest ⇒
      self forward JobRequestWithPromise(request, None)

    case JobRequestWithPromise(request, promise) ⇒
      log.info(s"ClientExecutor received from sender: ${sender.path.toString}")
      context.actorOf(SingleJobProcessor.props(broker, sender, request, promise, jobCounter), s"SingleJobProcessor-$jobCounter")
      context become waitForRequests(broker, jobCounter + 1, jobsOutstanding + jobCounter)

    case ShutDown ⇒ shutDown(broker)

    case JobCompleteIndex(finishedIndex) ⇒
      log.info(s"Received forwarded completion for job: $finishedIndex")
      val jobsRemaining = jobsOutstanding - finishedIndex
      log.info(s"Jobs left to complete: $jobsRemaining")
      context become waitForRequests(broker, jobCounter, jobsRemaining)

  }

  def shutDown(broker: ActorRef): Unit = {
    log.info(s"Coordinator shutting down...")
    broker ! ShutDown
    self ! PoisonPill
  }
}

