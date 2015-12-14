package springnz.sparkplug.client

import java.net.InetAddress

import better.files._
import com.typesafe.config.ConfigFactory
import springnz.util.{ BuilderOps, Pimpers, Logging }
import org.apache.spark.launcher.SparkLauncher

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Properties, Try }

object Launcher extends Logging {
  import Pimpers._

  val config = ConfigFactory.load().getConfig("sparkPlugClient.spark.conf")

  def startProcess(launcher: SparkLauncher): Future[Unit] = {
    val processFuture = Future {
      launcher.launch()
    }.withErrorLog("Failed to launch: ")
    processFuture.flatMap {
      process ⇒ executeProcess(process)
    }
  }

  private def executeProcess(process: Process): Future[Unit] = Future {
    val errorStream = scala.io.Source.fromInputStream(process.getErrorStream)
    val errorLines = errorStream.getLines()
    for (line ← errorLines) {
      log.info(line)
    }
    process.waitFor()
  }

  def launch(clientAkkaAddress: String, jarPath: File, mainJarPattern: String, mainClass: String, sendJars: Boolean = true): Try[Future[Unit]] = Try {

    val fullExtraJarFolder = jarPath.fullPath

    val sparkHome = Properties.envOrNone("SPARK_HOME")
    val sparkMaster = Properties.envOrElse("SPARK_MASTER", s"spark://${InetAddress.getLocalHost.getHostAddress}:7077")

    // TODO: enable this functionality (need Spark 1.5 for this)
    //    val sparkArgs: Array[String] = config.getString("spark.submit.sparkargs").split(' ')

    if (!sparkMaster.startsWith("local[") && !sparkHome.isDefined)
      throw new RuntimeException("If 'SPARK_MASTER' is not set to local, 'SPARK_HOME' must be set.")

    val appName = mainClass.split('.').last

    import BuilderOps._

    val mainJar = jarPath.glob(mainJarPattern).collectFirst { case f ⇒ f.fullPath }

    val launcher = (new SparkLauncher)
      .setIfSome[String](mainJar, (l, mj) ⇒ l.setAppResource(mj))
      .setMainClass(mainClass)
      .setAppName(appName)
      .setMaster(sparkMaster)
      .setIfSome[String](sparkHome, (l, sh) ⇒ l.setSparkHome(sh))
      .addAppArgs("SparkPlugExecutor", clientAkkaAddress)
      .setConf("spark.app.id", appName)
      .setConf(SparkLauncher.EXECUTOR_MEMORY, config.getString("spark.executor.memory"))
      .setConf(SparkLauncher.EXECUTOR_CORES, config.getString("spark.executor.cores"))
      .setConf(SparkLauncher.DRIVER_MEMORY, config.getString("spark.driver.memory"))
      .setDeployMode(config.getString("spark.deploymode"))
    //      .addSparkArgs(sparkArgs) TODO: enable this functionality (need Spark 1.5 for this)

    val extraJarFiles = jarPath.glob("**/*.jar").collect { case f ⇒ f.fullPath }.filterNot(_.contains("/akka-"))

    val launcherWithJars =
      if (sendJars)
        extraJarFiles.foldLeft(launcher) { case (l, jarFile) ⇒ l.addJar(jarFile) }
      else if (extraJarFiles.length == 0) launcher
      else launcher
        .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, s"$fullExtraJarFolder/*")
        .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, s"$fullExtraJarFolder/*")

    startProcess(launcherWithJars)
  }

}
