package springnz.sparkplug.client

import java.net.InetAddress

import better.files._
import com.typesafe.config.ConfigFactory
import springnz.util.{ Pimpers, Logging }
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

  implicit class LauncherSetter(launcher: SparkLauncher) {
    def setIfDefined(setFunc: (SparkLauncher, String) ⇒ SparkLauncher)(option: Option[String]): SparkLauncher =
      if (option.isDefined) setFunc(launcher, option.get) else launcher
  }

  def launch(clientAkkaAddress: String, jarPath: String, mainJar: String, mainClass: String, sendJars: Boolean = true): Try[Future[Unit]] = Try {

    val userDir = root / System.getProperty("user.dir")
    val fullMainJar = (userDir / jarPath / mainJar).fullPath
    val extraJarPath = userDir / jarPath
    val fullExtraJarFolder = extraJarPath.fullPath

    val sparkHome = Properties.envOrNone("SPARK_HOME")
    val sparkMaster = Properties.envOrElse("SPARK_MASTER", s"spark://${InetAddress.getLocalHost.getHostAddress}:7077")

    // TODO: enable this functionality (need Spark 1.5 for this)
    //    val sparkArgs: Array[String] = config.getString("spark.submit.sparkargs").split(' ')

    if (!sparkMaster.startsWith("local[") && !sparkHome.isDefined)
      throw new RuntimeException("If 'SPARK_MASTER' is not set to local, 'SPARK_HOME' must be set.")

    val appName = mainClass.split('.').last

    val launcher = (new SparkLauncher)
      .setAppResource(fullMainJar)
      .setMainClass(mainClass)
      .setAppName(appName)
      .setMaster(sparkMaster)
      .setIfDefined(_.setSparkHome(_))(sparkHome)
      .addAppArgs(clientAkkaAddress)
      .setConf(SparkLauncher.EXECUTOR_MEMORY, config.getString("spark.executor.memory"))
      .setConf(SparkLauncher.EXECUTOR_CORES, config.getString("spark.executor.cores"))
      .setConf(SparkLauncher.DRIVER_MEMORY, config.getString("spark.driver.memory"))
      .setDeployMode(config.getString("spark.deploymode"))
    //      .addSparkArgs(sparkArgs) TODO: enable this functionality (need Spark 1.5 for this)

    val extraJarFiles = extraJarPath.glob("**/*.jar").collect { case f ⇒ f.fullPath }.filterNot(_.contains("/akka-"))

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
