package springnz.sparkplug.client

import java.net.{ URLEncoder, InetAddress }

import better.files._
import com.typesafe.config.{ ConfigRenderOptions, Config }
import org.apache.spark.launcher.SparkLauncher
import springnz.sparkplug.util.{ BuilderOps, ConfigUtils, Logging, Pimpers }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Properties, Try }

object Launcher extends Logging {
  import BuilderOps._
  import Pimpers._

  def startProcess(launcher: SparkLauncher): Future[Unit] = {
    val processFuture = Future {
      launcher.launch()
    }.withErrorLog("Failed to launch: ")
    processFuture.flatMap {
      process ⇒ executeProcess(process)
    }
  }

  private def executeProcess(process: Process): Future[Unit] = Future {
    val outStream = scala.io.Source.fromInputStream(process.getInputStream)
    for (line ← outStream.getLines()) {
      log.info(line)
    }
    val errorStream = scala.io.Source.fromInputStream(process.getErrorStream)
    for (line ← errorStream.getLines()) {
      log.info(line)
    }
    process.waitFor()
  }

  def launch(clientAkkaAddress: String,
    jarPath: File,
    mainJarPattern: String,
    mainClass: String,
    sparkConfig: Config,
    akkaRemoteConfig: Option[Config],
    sendJars: Boolean = true): Try[Future[Unit]] = Try {

    val fullExtraJarFolder = jarPath.pathAsString

    val sparkHome = Properties.envOrNone("SPARK_HOME")
    val sparkMaster = Properties.envOrElse("SPARK_MASTER", s"spark://${InetAddress.getLocalHost.getHostAddress}:7077")
    log.debug(s"Spark master set to: $sparkMaster")

    // TODO: enable this functionality (need Spark 1.5 for this)
    //    val sparkArgs: Array[String] = config.getString("spark.submit.sparkargs").split(' ')

    if (!sparkMaster.startsWith("local[") && !sparkHome.isDefined)
      throw new RuntimeException("If 'SPARK_MASTER' is not set to local, 'SPARK_HOME' must be set.")

    val appName = mainClass.split('.').last

    val mainJar = jarPath.glob(mainJarPattern).collectFirst { case f ⇒ f.pathAsString }

    val configVars: Seq[(String, String)] = ConfigUtils.configFields(sparkConfig).toSeq

    val akkaRemoteConfigString = akkaRemoteConfig.map { config ⇒
      val configString = config.root().render(ConfigRenderOptions.concise())
      URLEncoder.encode(configString, "UTF-8")
    }

    val launcher = (new SparkLauncher)
      .setIfSome[String](mainJar) { (l, mj) ⇒ l.setAppResource(mj) }
      .setMainClass(mainClass)
      .setAppName(appName)
      .setMaster("spark://aklkvm026:7077")//sparkMaster)
      .setIfSome[String](sparkHome) { (l, sh) ⇒ l.setSparkHome(sh) }
      .addAppArgs("--appName", appName)
      .addAppArgs("--clientAkkaAddress", clientAkkaAddress)
      .setIfSome(akkaRemoteConfigString) { (l, config) ⇒ l.addAppArgs("--remoteAkkaConfig", config) }
      .setFoldLeft(configVars) { case (launcher, (key, value)) ⇒ launcher.setConf(key, value) }
      .setDeployMode("cluster") //sparkConfig.getString("spark.deploymode"))

    val extraJarFiles = jarPath.glob("*.jar")
      .map { case f ⇒ f.pathAsString }
      .filterNot(_.contains("/akka-"))

    val launcherWithJars =
      if (sendJars)
        extraJarFiles.foldLeft(launcher) { case (l, jarFile) ⇒ l.addJar(jarFile) }
      else if (extraJarFiles.length == 0) launcher
      else launcher
        .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, s"$fullExtraJarFolder/*")
        .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, s"$fullExtraJarFolder/*")

    launcherWithJars.setVerbose(true)

    startProcess(launcherWithJars)
  }

}
