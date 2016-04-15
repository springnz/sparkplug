package springnz.sparkplug.executor

import java.net.{ URLDecoder, URLEncoder }
import java.time.LocalDate

import akka.actor._
import com.typesafe.config.ConfigFactory
import springnz.sparkplug.core._
import springnz.sparkplug.util.Logging

import scala.util.{ Properties, Try }

object Constants {
  val defaultAkkaRemoteConfigSection = "akkaRemote"
  val actorSystemName = "sparkplugExecutorSystem"
  val brokerActorName = "sparkplugRequestBroker"
}

object ExecutorService extends Logging {
  import Constants._

  lazy val defaultRemoteAkkaConfig = ConfigFactory.load.getConfig(s"sparkplug.$defaultAkkaRemoteConfigSection")

  // TODO: proper command line parsing to allow richer config options
  def main(args: Array[String]): Unit = {
    if (args.length < 4)
      throw new IllegalArgumentException(s"Expected at least 4 arguments to ExecutorService. Args = : ${args.toList}")
    val appName = args(1)
    val sparkClientPath = args(3)

    log.info(s"Starting Sparkplug ExecutorService: SparkClient = $sparkClientPath: ${LocalDate.now()}")

    val remoteConfig = if (args.length == 6) {
      val urlEncodedConfig = args(5)
      val configString = URLDecoder.decode(urlEncodedConfig, "UTF-8")
      val config = ConfigFactory.parseString(configString)
      log.info(s"Using akka remote config:\n$configString")
      config
    } else {
      log.info(s"Using default akka remote config from config section 'sparkplug.$defaultAkkaRemoteConfigSection'")
      defaultRemoteAkkaConfig
    }

    import scala.collection.JavaConversions._
    def env = System.getenv().toMap
    log.debug(s"Environment:\n $env")

    val system = ActorSystem(actorSystemName, remoteConfig)

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

