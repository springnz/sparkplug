package springnz.sparkplug

import akka.actor._
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest._
import springnz.sparkplug.executor.Constants
import springnz.sparkplug.executor.MessageTypes._

import scala.concurrent.duration._

class ExecutorServiceBrokerDeathTests(_system: ActorSystem)
    extends TestKit(_system) with ExecutorServiceBase with ImplicitSender with WordSpecLike with BeforeAndAfterAll {

  case object ServerTerminated

  def this() = this(ActorSystem("TestSystem", ConfigFactory.load().getConfig(Constants.defaultConfigSectionName)))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "deathwatch on client (base case)" in new ExecutorServiceFixture(self, "client1", "testBroker1") {
    expectMsg(1 seconds, ServerReady)
    val requestBroker = system.actorSelection(s"/user/testBroker1")
    // give it something to do for a while
    val request = JobRequest("springnz.sparkplug.examples.WaitPlugin", None)
    requestBroker ! request
    expectMsgType[JobSuccess](3 second)
  }

  // TODO: make it so this doesn't have to be the last test
  "deathwatch on client (with poison pill should terminate server)" in new ExecutorServiceFixture(self, "client2", "testBroker2") {
    expectMsg(1 seconds, ServerReady)
    val requestBroker = system.actorSelection(s"/user/testBroker2")
    // give it something to do for a while
    val request = JobRequest("springnz.sparkplug.examples.WaitPlugin", None)
    requestBroker ! request
    clientActor ! PoisonPill
    expectNoMsg(3 second)
  }
}
