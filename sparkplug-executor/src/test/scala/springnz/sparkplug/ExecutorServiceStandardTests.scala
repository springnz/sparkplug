package springnz.sparkplug

import akka.actor._
import akka.testkit.{ TestKitBase, ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest._
import springnz.sparkplug.core.{ SparkPlugException, Configurer, LocalConfigurer }
import springnz.sparkplug.executor.MessageTypes._
import springnz.sparkplug.executor.{ Constants, ExecutorService }

import scala.concurrent.duration._

class ExecutorServiceStandardTests(_system: ActorSystem)
    extends TestKit(_system) with ExecutorServiceBase with ImplicitSender with WordSpecLike with BeforeAndAfterAll {

  case object ServerTerminated

  def this() = this(ActorSystem("TestSystem", ConfigFactory.load().getConfig(Constants.defaultConfigSectionName)))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "notify the client that server is ready" in new ExecutorServiceFixture(self, "client1", "testBroker1") {
    expectMsg(1 seconds, ServerReady)
  }

  "successfuly execute a job request via a plugin" in new ExecutorServiceFixture(self, "client2", "testBroker2") {
    expectMsg(1 seconds, ServerReady)
    val requestBroker = system.actorSelection(s"/user/testBroker2")
    val request = JobRequest("springnz.sparkplug.examples.LetterCountPlugin", None)
    requestBroker ! request
    expectMsg[JobSuccess](6 seconds, JobSuccess(request, (2, 2)))
  }

  "deathwatch on job processor should produce jobfailed message" in new ExecutorServiceFixture(self, "client3", "testBroker3") {
    expectMsg(1 seconds, ServerReady)
    import scala.concurrent._
    val requestBroker = system.actorSelection(s"/user/testBroker3")
    // give it something to do for a while
    val request = JobRequest("springnz.sparkplug.examples.WaitPlugin", Some(6000))
    requestBroker ! request
    blocking { Thread.sleep(1000) }
    system.actorSelection(s"/user/testBroker3/jobProcessor-0") ! PoisonPill
    blocking { Thread.sleep(1000) }
    expectMsgType[JobFailure](3 seconds)
  }
}
