package springnz.sparkplug.executor

import akka.actor.TypedActor.{ PreStart, PostStop }
import akka.actor._
import better.files._
import springnz.sparkplug.executor.InternalMessageTypes.RoutedRequest
import springnz.sparkplug.executor.MessageTypes.{ ShutDown, ServerReady, JobRequest }
import org.apache.spark.SparkContext
import org.joda.time.DateTime

class RequestBroker(sparkClient: String, postStopAction: ⇒ Unit)(implicit sparkContext: SparkContext)
    extends Actor with PreStart with PostStop with ActorLogging {

  override def preStart() = {
    s"XXX: RequestBroker preStart: ${DateTime.now()}" >>: (home / "testfile.txt")
    // logging Spark Configuration
    val sparkConfDebug = sparkContext.getConf
      .toDebugString
      .lines
      .filterNot(_.contains("password"))
      .mkString("\n")
    log.info(s"Spark-Configuration:\n$sparkConfDebug\n")

    log.info(s"Notifying client at address '$sparkClient' of readiness.")
    if (sparkClient.nonEmpty) {
      s"XXX: Trying to contact sparkClient $sparkClient: ${DateTime.now()}" >>: (home / "testfile.txt")
      val clientActor = context.actorSelection(sparkClient)
      s"XXX: Sending to sparkClient $sparkClient: ${DateTime.now()}" >>: (home / "testfile.txt")
      clientActor ! ServerReady
    }
    log.info("Finished loading RequestBroker. Ready for action.")
  }

  override def postStop() = {
    log.info("Running postStopAction")
    postStopAction
  }

  def receive = receiveJobRequests(0)

  def receiveJobRequests(jobCount: Int): Receive = {
    case job: JobRequest ⇒
      log.info(s"Creating new jobProcessor for '${job.factoryClassName}'.")
      val processor = context.actorOf(JobProcessor.props, s"jobProcessor-$jobCount")
      context.become(receiveJobRequests(jobCount + 1))
      processor ! RoutedRequest(job, sender)

    case ShutDown ⇒
      log.info(s"Shutting down...")
      context.system.shutdown()
  }
}

