package springnz.sparkplug.executor

import akka.actor._
import better.files._
import com.typesafe.config.ConfigFactory

import springnz.sparkplug.core._
import springnz.util.Logging
import org.joda.time.DateTime

import scala.util.{ Properties, Try }

object Constants {
  val defaultConfigSectionName = "sparkPlugAkkaExecutorService"
  val actorSystemName = "sparkPlugExecutorSystem"
  val brokerActorName = "sparkPlugRequestBroker"
  val sparkAppName = "SparkPlugExecutor"
}

object ExecutorService extends Logging {
  import Constants._

  def main(args: Array[String]): Unit = {
    if (args.length != 1)
      throw new IllegalArgumentException("Expected 1 arguments to ExecutorService, the address of the.")
    val sparkClientPath = args(0)

    log.info(s"Starting Sparkplug ExecutorService: SparkClient = $sparkClientPath: ${DateTime.now()}")

    val executorService = new ExecutorService
    val executorConfig = ConfigFactory.load().getConfig(defaultConfigSectionName)
    val system = ActorSystem(actorSystemName, executorConfig)

    executorService.start(system, sparkClientPath)
  }
}

class ExecutorService extends LongLivedExecutor with Logging {
  import Constants._

  override val configurer: Configurer = new LocalConfigurer(sparkAppName, Properties.envOrNone("SPARK_MASTER"))

  def start(system: ActorSystem, sparkClientPath: String): Try[Unit] = {

    val actorOperation = SparkOperation[Unit] { implicit sparkContext ⇒

      def postStopAction() = {
        log.info("Stopping Spark context.")
        sparkContext.stop()
      }

      log.info("Creating requestBroker for ExecutorService.")
      system.actorOf(Props(new RequestBroker(sparkClientPath, postStopAction)), name = brokerActorName)
    }

    log.info("Executing container operation (everything happens inside this method).")
    val result = execute(actorOperation)
    log.info("Finished executing container operation (everything happens inside this method).")
    result
  }

}

