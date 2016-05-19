package springnz.sparkplug.client

import akka.actor.TypedActor.PreStart
import akka.actor._
import better.files.File._
import better.files._
import com.typesafe.config.Config
import springnz.sparkplug.executor.MessageTypes._
import springnz.sparkplug.util.Logging

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

object Coordinator {
  private[client] case class JobRequestWithPromise(jobRequest: JobRequest, promise: Option[Promise[Any]])
  private[client] case class JobCompleteIndex(jobIndex: Int)

  case class JobWaitingDetails(sender: ActorRef, request: JobRequest)
  case class JobRequestDetails(jobIndex: Int, sender: ActorRef, request: JobRequest)
  private case object RestartLauncher

  private case class LauncherError(reason: Throwable)

  def props(readyPromise: Option[Promise[ActorRef]],
    akkaClientConfig: Config = ClientExecutor.defaultClientAkkaConfig,
    sparkConfig: Config = ClientExecutor.defaultSparkConfig,
    akkaRemoteConfig: Option[Config] = None,
    jarPath: Option[String] = None) =
    Props(new Coordinator(readyPromise, akkaClientConfig, sparkConfig, akkaRemoteConfig, jarPath))
}

class Coordinator(
    readyPromise: Option[Promise[ActorRef]],
    akkaClientConfig: Config,
    sparkConfig: Config,
    akkaRemoteConfig: Option[Config],
    jarPath: Option[String]) extends Actor with PreStart with Logging {
  import Constants._
  import Coordinator._

  private val restartAttempts = akkaClientConfig.getInt("restart.attempts")
  private val restartDelayIncrements = akkaClientConfig.getInt("restart.delayincrements")

  private var restartCount = 0

  override def preStart() = startLauncher()

  def startLauncher() = {
    val launchTry: Try[Future[Unit]] = Try {
      // doesn't seem to be a way to get the hostname and port at runtime
      val hostName = akkaClientConfig.getString("akka.remote.netty.tcp.hostname")
      val port = akkaClientConfig.getInt("akka.remote.netty.tcp.port")
      val systemName = context.system.name
      val clientPath = context.self.path.toString
      val localPath = clientPath.substring(clientPath.indexOf("/user/"))
      val clientAkkaAddress = s"akka.tcp://$systemName@$hostName:$port$localPath"

      def userDir: File = root / System.getProperty("user.dir")
      val usedJarPathSuffix = jarPath.getOrElse(defaultJarPath)
      val usedJarPath = if (usedJarPathSuffix.startsWith("/")) root / usedJarPathSuffix else userDir / usedJarPathSuffix

      Launcher.launch(clientAkkaAddress, usedJarPath, mainJarPattern, mainClass, sparkConfig, akkaRemoteConfig).get
    }

    launchTry match {
      case Failure(reason) ⇒
        log.error(s"Error was caught with in Coordinator preStart (at Launcher setup phase)", reason)
        self ! LauncherError(reason)

      case Success(future) ⇒
        implicit val executionContext = context.system.dispatcher
        future.onFailure {
          case reason ⇒
            log.error(s"Error was caught with in Coordinator preStart (at Launcher execution phase)", reason)
            self ! LauncherError(reason)
        }
    }
  }

  override def receive: Receive = waitForReady(0, List.empty)

  def waitForReady(jobCounter: Int, queuedList: List[JobWaitingDetails]): Receive = {
    case LauncherError(reason) ⇒
      log.error(s"Shutting down coordinator after launcher error.", reason)
      if (readyPromise.isDefined)
        readyPromise.get.failure(reason)
      else
        context.parent ! ServerError(reason)
      context.become(notReceivingRequests)
      self ! PoisonPill

    case ServerReady ⇒
      val broker = sender()
      context.become(waitForRequests(broker, jobCounter, Set.empty))
      log.info(s"Coordinator received a ServerReady message from broker: ${broker.path.toString}")

      // Compete the Promise or send ServerReady
      if (readyPromise.isDefined)
        readyPromise.get.complete(Success(self))
      else
        context.parent ! ServerReady

      // death watch on the server
      context.watch(broker)
      // Tell the Server the client is ready (so the server can DeathWatch on the client)
      log.info(s"Coordinator tells client it's ready")
      broker ! ClientReady

      queuedList.foreach {
        case JobWaitingDetails(originalSender, request) ⇒
          log.info(s"Forwarding queued request $request from $sender")
          self.tell(request, originalSender)
      }

    case ServerError(reason) ⇒ {
      log.error(s"Received an error from the Server.", reason)
      // TODO: handle this
    }

    case request: JobRequest ⇒ {
      log.info(s"Queueing request $request from $sender")
      context.become(waitForReady(jobCounter, JobWaitingDetails(sender, request) :: queuedList))
    }

    case RestartLauncher ⇒ {
      log.info(s"Restarting the server for the ${restartAttempts + 1} (th) time.")
      restartCount += 1
      startLauncher
    }
  }

  def waitForRequests(broker: ActorRef, jobCounter: Int, jobsOutstanding: Set[JobRequestDetails]): Receive = {

    case request: JobRequest ⇒
      self forward JobRequestWithPromise(request, None)

    case JobRequestWithPromise(request, promise) ⇒
      // Either complete the promise or reply to the sender (not both)
      val requestor = promise match {
        case None ⇒
          log.info(s"Coordinator received job request from sender: ${sender.path.toString}")
          Some(sender)

        case _ ⇒
          log.info(s"Coordinator received job request from ClientExecutor (via Future interface)")
          None
      }
      context.actorOf(SingleJobProcessor.props(request, broker, requestor, promise, jobCounter), s"SingleJobProcessor-$jobCounter")
      context become waitForRequests(broker, jobCounter + 1, jobsOutstanding + JobRequestDetails(jobCounter, sender, request))

    case ShutDown ⇒ shutDown(broker)

    case JobCompleteIndex(finishedIndex) ⇒
      log.info(s"Received forwarded completion for job: $finishedIndex")
      val finishedJob: Option[JobRequestDetails] = jobsOutstanding.find { details ⇒ details.jobIndex == finishedIndex }
      val jobsRemaining = finishedJob.map(job ⇒ jobsOutstanding - job).getOrElse(jobsOutstanding)
      log.info(s"Jobs left to complete: $jobsRemaining")
      context become waitForRequests(broker, jobCounter, jobsRemaining)

    case Terminated(_) ⇒
      log.info("Server terminated, crashed or timed out. Shutting down the client coordination.")

      val jobsToResubmit = jobsOutstanding.toList.map(details ⇒ JobWaitingDetails(details.sender, details.request))
      context.become(waitForReady(jobCounter, jobsToResubmit))
      if (restartCount < restartAttempts) {
        val rd = (restartCount + 1) * restartDelayIncrements
        implicit val executionContext = context.system.dispatcher
        context.system.scheduler.scheduleOnce(rd.seconds, self, RestartLauncher)
      } else {
        log.error(s"Maximum number of restarts reached: $restartAttempts")
        context.become(notReceivingRequests)
      }
  }

  def notReceivingRequests(): Receive = {
    case JobRequestWithPromise(request, promise) ⇒
      val reason = new SparkplugException(s"Sparkplug server is down and/or reached the maximum restart attempts. Request '${request.factoryClassName}' not processed.")
      if (readyPromise.isDefined)
        readyPromise.get.failure(reason)
      else
        sender ! ServerError(reason)

    case _ ⇒
  }

  def shutDown(broker: ActorRef): Unit = {
    log.info(s"Coordinator shutting down...")
    broker ! ShutDown
    context.become(notReceivingRequests)
    self ! PoisonPill
  }

}

