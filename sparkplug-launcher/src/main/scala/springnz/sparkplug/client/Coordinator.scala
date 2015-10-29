package springnz.sparkplug.client

import akka.actor.TypedActor.PreStart
import akka.actor._
import com.typesafe.config.{ Config, ConfigFactory }
import springnz.sparkplug.client.Constants._
import springnz.sparkplug.client.Coordinator.JobRequestWithPromise
import springnz.sparkplug.executor.MessageTypes._
import springnz.util.Logging

import scala.concurrent._
import scala.util.{ Failure, Try, Success }

object Coordinator {
  case class JobRequestWithPromise(jobRequest: JobRequest, promise: Option[Promise[Any]])
  case class JobCompleteIndex(jobIndex: Int)

  def props(readyPromise: Promise[ActorRef]) = Props(new Coordinator(Some(readyPromise)))

  def defaultConfig: Config = {
    val config = ConfigFactory.load()
    config.getConfig(defaultConfigSectionName)
  }
}

class Coordinator(readyPromise: Option[Promise[ActorRef]] = None, config: Config = Coordinator.defaultConfig) extends Actor with PreStart with Logging {
  import Coordinator._
  import Constants._

  override def preStart() = {
    def terminateStartup(reason: Throwable): Unit = {
      context.parent ! ServerError(reason)
      readyPromise.foreach(_.failure(reason))
      self ! PoisonPill
    }

    val launchTry: Try[Future[Unit]] = Try {
      // doesn't seem to be a way to get the hostname and port at runtime
      val hostName = config.getString("akka.remote.netty.tcp.hostname")
      val port = config.getInt("akka.remote.netty.tcp.port")
      val systemName = context.system.name
      val clientPath = context.self.path.toString
      val localPath = clientPath.substring(clientPath.indexOf("/user/"))
      val clientAkkaAddress = s"akka.tcp://$systemName@$hostName:$port$localPath"
      Launcher.launch(clientAkkaAddress, jarPath, mainJar, mainClass).get
    }

    launchTry match {
      case Failure(reason) ⇒
        log.error(s"Error was caught with in Coordinator preStart (at Launcher setup phase): ${reason.toString}")
        terminateStartup(reason)

      case Success(future) ⇒
        implicit val executionContext = context.system.dispatcher
        future.onFailure {
          case reason ⇒
            log.error(s"Error was caught with in Coordinator preStart (at Launcher execution phase): ${reason.toString}")
            terminateStartup(reason)
        }
    }
  }

  override def receive: Receive = waitForReady(List.empty)

  def waitForReady(queuedList: List[(ActorRef, JobRequest)]): Receive = {
    case ServerReady ⇒
      val broker = sender
      context.become(waitForRequests(broker, 0, Set.empty[Int]))
      log.info(s"Coordinator received a ServerReady message from broker: ${broker.path.toString}")
      context.parent ! ServerReady
      readyPromise.foreach(_.complete(Success(self)))
      queuedList.foreach {
        case (originalSender, request) ⇒
          log.info(s"Forwarding queued request $request from $sender")
          self.tell(request, originalSender)
      }

    case ServerError(reason) ⇒ {
      log.error(s"Received an error from the Server: $reason")
      // TODO: handle this
    }

    case request: JobRequest ⇒ {
      log.info(s"Queueing request $request from $sender")
      context.become(waitForReady((sender, request) :: queuedList))
    }
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

  def notReceivingRequests(): Receive = {
    case _ ⇒
  }

  def shutDown(broker: ActorRef): Unit = {
    log.info(s"Coordinator shutting down...")
    broker ! ShutDown
    context.become(notReceivingRequests)
    self ! PoisonPill
  }
}

