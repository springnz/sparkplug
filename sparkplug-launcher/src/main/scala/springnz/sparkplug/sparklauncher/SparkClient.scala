package springnz.sparkplug.executor

import akka.actor.TypedActor.PreStart
import akka.actor._
import com.typesafe.config.ConfigFactory
import springnz.sparkplug.executor.MessageTypes._
import springnz.sparkplug.sparklauncher.Launcher
import springnz.util.Logging

import scala.concurrent._
import scala.util.Success

object SparkClient {

  // Constants
  val actorSystemName = "SparkActorSystem"
  val clientActorName = "sparkClient"
  val configSectionName = "sparkPlugAkkaClient"
  val jarPath = "target/pack/lib"
  val mainJar = s"$jarPath/sparkplug-executor_2.11-0.2.6-SNAPSHOT.jar"
  val mainClass = "springnz.sparkplug.executor.ExecutorService"

  def main(args: Array[String]): Unit = {
    new SparkClient
  }
}

trait SparkClientExecutor {
  def execute[A](jarPath: String, pluginJar: String, pluginClass: String): Future[A]

}

class SparkClient {
  import SparkClient._

  val readyPromise = Promise[ActorRef]()

  val actorSystem = ActorSystem(actorSystemName, ConfigFactory.load().getConfig(configSectionName))
  actorSystem.actorOf(Props(new SparkClientActor(readyPromise)), name = clientActorName)

  val clientActor: Future[ActorRef] = readyPromise.future

  implicit val ec = actorSystem.dispatcher
  clientActor.map {
    actorRef ⇒
      actorRef ! JobRequest("springnz.sparkplug.examples.LetterCountPlugin", None)
  }
}

class SparkClientActor(readyPromise: Promise[ActorRef]) extends Actor with PreStart with Logging {
  import SparkClient._

  override def preStart() = {
    val config = ConfigFactory.load()
    val hostName = config.getString(s"$configSectionName.akka.remote.netty.tcp.hostname")
    val port = config.getInt(s"$configSectionName.akka.remote.netty.tcp.port")

    val clientPath = s"akka.tcp://$actorSystemName@$hostName:$port/user/$clientActorName"
    Launcher(clientPath,
      mainJar,
      mainClass,
      extraJarFolder = jarPath)
  }

  override def receive: Receive = {
    case ServerReady ⇒
      val broker = sender
      context.become(waitForRequests(broker))
      readyPromise.complete(Success(self))
  }

  def waitForRequests(broker: ActorRef): Receive = {

    case request: JobRequest ⇒
      log.info(s"ClientExecutor received from sender: $sender")
      broker ! request

    case ShutDown ⇒
      log.info(s"ClientExecutor shutting down...")
      broker ! ShutDown
      context.system.shutdown()

    case JobSuccess(jobRequest, result) ⇒
      log.info(s"Received Result from sender: $sender")
      log.info(s"Result value: $result")
      self ! ShutDown
  }
}

