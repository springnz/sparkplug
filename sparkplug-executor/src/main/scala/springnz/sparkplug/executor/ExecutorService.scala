package springnz.sparkplug.executor

import akka.actor._
import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import springnz.sparkplug.core._
import springnz.util.Logging

import scala.util.{ Properties, Try }

object Constants {
  val defaultConfigSectionName = "sparkPlugAkkaExecutorService"
  val actorSystemName = "sparkPlugExecutorSystem"
  val brokerActorName = "sparkPlugRequestBroker"
}

object ExecutorService extends Logging {
  import Constants._

  // TODO: proper command line parsing to allow richer config options
  def main(args: Array[String]): Unit = {
    if (args.length != 2)
      throw new IllegalArgumentException(s"Expected 2 arguments to ExecutorService, the app name and the akka address of the client actor. Args = : ${args.toList}")
    val appName = args(0)
    val sparkClientPath = args(1)

    log.info(s"Starting Sparkplug ExecutorService: SparkClient = $sparkClientPath: ${DateTime.now()}")

    val executorConfig = ConfigFactory.load().getConfig(defaultConfigSectionName)
    val system = ActorSystem(actorSystemName, executorConfig)

    val executorService = new ExecutorService(appName)
    executorService.start(system, sparkClientPath)
  }
}

class ExecutorService(appName: String, brokerName: String = Constants.brokerActorName) extends LongLivedExecutor with Logging {

  override val configurer: Configurer = new LocalConfigurer(appName, Properties.envOrNone("SPARK_MASTER"))

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

