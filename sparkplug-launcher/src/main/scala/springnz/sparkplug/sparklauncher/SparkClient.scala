package springnz.sparkplug.executor

import akka.actor.TypedActor.PreStart
import akka.actor._
import com.typesafe.config.ConfigFactory
import springnz.sparkplug.executor.MessageTypes._
import springnz.sparkplug.sparklauncher.Launcher
import springnz.util.Logging

object SparkClient {

  // Constants
  val actorSystemName = "SparkActorSystem"
  val clientActorName = "sparkClient"
  val configSectionName = "sparkAkkaClient"
  val jarPath = "target/pack/lib"
  val mainJar = s"$jarPath/spark-executor_2.11-0.2.4-SNAPSHOT.jar"
  val mainClass = "springnz.sparkplug.executor.ExecutorService"

  def main(args: Array[String]): Unit = {
    val system = ActorSystem(actorSystemName, ConfigFactory.load().getConfig(configSectionName))
    system.actorOf(Props(new SparkClient), name = clientActorName)
  }
}

class SparkClient() extends Actor with PreStart with Logging {
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
      context.become(waitForRequests(broker, 5))
      sendJobRequest(broker)
  }

  def sendJobRequest(broker: ActorRef) = self.tell(JobRequest("springnz.sparkplug.examples.LetterCountFactory", None), broker)

  def waitForRequests(broker: ActorRef, count: Int): Receive = {

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
      if (count > 0) {
        context.become(waitForRequests(broker, count - 1))
        sendJobRequest(broker)
      } else
        self ! ShutDown
  }
}

