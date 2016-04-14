package springnz.sparkplug.executor

import java.time.LocalDate

import akka.actor._
import com.typesafe.config.ConfigFactory
import springnz.sparkplug.core._
import springnz.sparkplug.util.Logging

import scala.util.{ Properties, Try }

object Constants {
  val akkaConfigSection = "sparkPlugAkkaExecutorService"
  val actorSystemName = "sparkPlugExecutorSystem"
  val brokerActorName = "sparkPlugRequestBroker"
}

object ExecutorService extends Logging {
  import Constants._

  lazy val defaultRemoteAkkaConfig = ConfigEnvironment.config.getConfig(akkaConfigSection)

  // TODO: proper command line parsing to allow richer config options
  def main(args: Array[String]): Unit = {
    if (args.length != 2)
      throw new IllegalArgumentException(s"Expected 2 arguments to ExecutorService, the app name and the akka address of the client actor. Args = : ${args.toList}")
    val appName = args(0)
    val sparkClientPath = args(1)

    log.info(s"Starting Sparkplug ExecutorService: SparkClient = $sparkClientPath: ${LocalDate.now()}")

    import scala.collection.JavaConversions._
    def env = System.getenv().toMap
    log.debug(s"Environment:\n $env")

    val system = ActorSystem(actorSystemName, defaultRemoteAkkaConfig)

    val executorService = new ExecutorService(appName)
    executorService.start(system, sparkClientPath)
  }
}

class ExecutorService(appName: String, brokerName: String = Constants.brokerActorName) extends LongLivedExecutor with Logging {

  // Note that the SparkConf inherits all its settings from spark-submit
  override val configurer: Configurer = new LocalConfigurer(appName, Properties.envOrNone("SPARK_MASTER"), None)

  def start(system: ActorSystem, sparkClientPath: String): Try[Unit] = {

    val actorOperation = SparkOperation[Unit] { implicit sparkContext ⇒

      def postStopAction() = {
        log.info("Stopping Spark context.")
        sparkContext.stop()
      }

      log.info("Creating requestBroker for ExecutorService.")
      system.actorOf(Props(new RequestBroker(sparkClientPath, postStopAction)), name = brokerName)
    }

    log.info("Executing container operation (everything happens inside this method).")
    val result = execute(actorOperation)
    log.info("Finished executing container operation (everything happens inside this method).")
    result
  }

}

