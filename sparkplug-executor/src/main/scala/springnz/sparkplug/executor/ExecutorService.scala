package springnz.sparkplug.executor

import akka.actor._
import better.files._
import com.typesafe.config.ConfigFactory

import springnz.sparkplug.core._
import springnz.util.Logging
import org.joda.time.DateTime

import scala.util.{ Properties, Try }

object ExecutorService {
  def main(args: Array[String]): Unit = {
    if (args.length != 1)
      throw new IllegalArgumentException("Expected 1 arguments to ExecutorService, the address of the.")
    val sparkClientPath = args(0)

    s"XXX: SparkClient = $sparkClientPath: ${DateTime.now()}" >>: (home / "testfile.txt")

    val executorService = new ExecutorService
    val executorConfig = ConfigFactory.load().getConfig("sparkExecutorService")
    val system = ActorSystem("ExecutorService", executorConfig)

    executorService.start(system, sparkClientPath)
  }
}

class ExecutorService extends LongLivedExecutor with Logging {

  override val configurer: Configurer = new LocalConfigurer("ExecutorService", Properties.envOrNone("SPARK_MASTER"))

  def start(system: ActorSystem, sparkClientPath: String): Try[Unit] = {

    val actorOperation = SparkOperation[Unit] { implicit sparkContext ⇒

      def postStopAction() = {
        log.info("Stopping Spark context.")
        sparkContext.stop()
      }

      log.info("Creating requestBroker executor.")
      s"XXX: Creating RequestBroker: ${DateTime.now()}" >>: (home / "testfile.txt")
      system.actorOf(Props(new RequestBroker(sparkClientPath, postStopAction)), name = "requestBroker")
    }

    log.info("Executing container operation.")
    execute(actorOperation)
  }

}

