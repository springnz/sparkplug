package springnz.sparkplug

import akka.actor._
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest._
import springnz.sparkplug.examples.{ WaitPlugin, LetterCountPlugin }
import springnz.sparkplug.executor.Constants
import springnz.sparkplug.executor.MessageTypes._

import scala.concurrent._
import scala.concurrent.duration._

class ExecutorServiceStandardTests(_system: ActorSystem)
    extends TestKit(_system) with ExecutorServiceBase with ImplicitSender with WordSpecLike with BeforeAndAfterAll {

  case object ServerTerminated

  def this() = this(ActorSystem("TestSystemStandard", ConfigFactory.load().getConfig(Constants.defaultConfigSectionName)))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "notify the client that server is ready" in new ExecutorServiceFixture(self, "client1", "testBroker1") {
    expectMsg(3.seconds, ServerReady)
  }

  "successfuly execute a job request via a plugin" in new ExecutorServiceFixture(self, "client2", "testBroker2") {
    val requestBroker = system.actorSelection(s"/user/testBroker2")
    val request = JobRequest(() ⇒ new LetterCountPlugin)
    Await.ready(readyPromise.future, 3.seconds)
    requestBroker ! request
    expectMsg(3.seconds, ServerReady)
    expectMsg[JobSuccess](20.seconds, JobSuccess(request, (2, 2)))
  }

  "deathwatch on job processor should produce jobfailed message" in new ExecutorServiceFixture(self, "client3", "testBroker3") {
    import scala.concurrent._
    val requestBroker = system.actorSelection(s"/user/testBroker3")
    // give it something to do for a while
    val request = JobRequest(() ⇒ new WaitPlugin(5000))
    Await.ready(readyPromise.future, 3.seconds)
    requestBroker ! request
    blocking { Thread.sleep(1000) }
    system.actorSelection(s"/user/testBroker3/jobProcessor-0") ! PoisonPill
    blocking { Thread.sleep(1000) }
    expectMsg(3.seconds, ServerReady)
    expectMsgType[JobFailure](3.seconds)
  }
}
