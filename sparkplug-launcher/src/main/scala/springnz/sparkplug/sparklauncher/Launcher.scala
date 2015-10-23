package springnz.sparkplug.sparklauncher

import java.net.InetAddress

import better.files._
import com.typesafe.config.ConfigFactory
import springnz.util.{ Pimpers, Logging }
import org.apache.spark.launcher.SparkLauncher

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Properties, Try }

object Launcher extends Logging {

  val config = ConfigFactory.load()

  def executeProcess(launcher: SparkLauncher): Future[Unit] = {
    import Pimpers._

    val future: Future[Unit] = Future {
      Try {
        val process = launcher.launch()
        val errorStream = scala.io.Source.fromInputStream(process.getErrorStream)
        val errorLines = errorStream.getLines()
        for (line ← errorLines) {
          log.info(line)
        }
        process.waitFor()
      }.withErrorLog("Failed to Launch")
    }
    future
  }

  def apply(sparkClientAkkaAddress: String,
    mainJar: String,
    mainClass: String,
    extraJarFolder: String,
    sendJars: Boolean = true): Try[Future[Unit]] = Try {

    val userDir = root / System.getProperty("user.dir")
    val fullMainJar = (userDir / mainJar).fullPath
    val extraJarPath = userDir / extraJarFolder
    val fullExtraJarFolder = extraJarPath.fullPath

    val sparkHome = Properties.envOrNone("SPARK_HOME")
    val sparkMaster = Properties.envOrSome("SPARK_MASTER", Some(s"spark://${InetAddress.getLocalHost.getHostAddress}:7077"))

    val appName = mainClass.split('.').last

    val launcher = (new SparkLauncher)
      .setAppResource(fullMainJar)
      .setMainClass(mainClass)
      .setAppName(appName)
      .setMaster(sparkMaster.get)
      .setSparkHome(sparkHome.get)
      .addAppArgs(sparkClientAkkaAddress)
      .setConf(SparkLauncher.EXECUTOR_MEMORY, config.getString("spark.executor.memory"))
      .setConf(SparkLauncher.EXECUTOR_CORES, config.getString("spark.executor.cores"))
      .setConf(SparkLauncher.DRIVER_MEMORY, config.getString("spark.driver.memory"))

    val extraJarFiles = extraJarPath.glob("**/*.jar").collect { case f ⇒ f.fullPath }.filterNot(_.contains("/akka-"))

    val launcherWithJars =
      if (sendJars)
        extraJarFiles.foldLeft(launcher) { case (l, jarFile) ⇒ l.addJar(jarFile) }
      else if (extraJarFiles.length == 0) launcher
      else launcher
        .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, s"$fullExtraJarFolder/*")
        .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, s"$fullExtraJarFolder/*")

    executeProcess(launcherWithJars)
  }

}
