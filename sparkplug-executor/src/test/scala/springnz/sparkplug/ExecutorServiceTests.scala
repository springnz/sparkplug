package springnz.sparkplug

import akka.actor._
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest._
import springnz.sparkplug.core.{ SparkOperation, SparkPlugin, Configurer, LocalConfigurer }
import springnz.sparkplug.executor.MessageTypes.{ ClientReady, JobRequest, JobSuccess, ServerReady }
import springnz.sparkplug.executor.{ Constants, ExecutorService }

import scala.concurrent.duration._

class WaitPlugin extends SparkPlugin {
  override def apply(input: Any): SparkOperation[Unit] = SparkOperation { _ ⇒ Thread.sleep(2000) }
}

class ExecutorServiceTests(_system: ActorSystem)
    extends TestKit(_system) with ImplicitSender with WordSpecLike with BeforeAndAfterAll {

  case object ServerTerminated

  def this() = this(ActorSystem("TestSystem", ConfigFactory.load().getConfig(Constants.defaultConfigSectionName)))

  "notify the client that server is ready" in new Fixture(self, "client1", "testBroker1") {
    expectMsg(1 seconds, ServerReady)
  }

  "successfuly execute a job request via a plugin" in new Fixture(self, "client2", "testBroker2") {
    expectMsg(1 seconds, ServerReady)
    val requestBroker = system.actorSelection(s"/user/testBroker2")
    val request = JobRequest("springnz.sparkplug.examples.LetterCountPlugin", None)
    requestBroker ! request
    expectMsg[JobSuccess](6 seconds, JobSuccess(request, (2, 2)))
  }

  "deathwatch on client (base case)" in new Fixture(self, "client3", "testBroker3") {
    expectMsg(1 seconds, ServerReady)
    val requestBroker = system.actorSelection(s"/user/testBroker3")
    // give it something to do for a while
    val request = JobRequest("springnz.sparkplug.WaitPlugin", None)
    requestBroker ! request
    expectMsgType[JobSuccess](3 second)
  }

  "deathwatch on client (with poison pill should terminate server)" in new Fixture(self, "client4", "testBroker4") {
    expectMsg(1 seconds, ServerReady)
    val requestBroker = system.actorSelection(s"/user/testBroker4")
    // give it something to do for a while
    val request = JobRequest("springnz.sparkplug.WaitPlugin", None)
    requestBroker ! request
    clientActor ! PoisonPill
    expectNoMsg(3 second)
  }

  class Fixture(probe: ActorRef, clientName: String, brokerName: String) {

    val executorService = new ExecutorService("TestService", brokerName) {
      // Run it locally in Spark
      override val configurer: Configurer = new LocalConfigurer("TestService", None)
    }

    executorService.start(system, s"/user/$clientName")

    val clientActor = system.actorOf(Props(new Actor {
      override def receive = {
        case ServerReady ⇒
          probe forward ServerReady
          sender ! ClientReady

        case Terminated(actor) ⇒
          probe ! ServerTerminated
      }
    }), clientName)
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

}

//
//class ExecutorServiceTests2 extends fixture.WordSpec with ShouldMatchers with Logging with Inspectors {
//  implicit val ec = scala.concurrent.ExecutionContext.global
//
//  override type FixtureParam = Fixture
//
//  override def withFixture(test: OneArgTest) = {
//    var system: ActorSystem = ActorSystem("TestSystem", ConfigFactory.load().getConfig(Constants.defaultConfigSectionName))
//
//    try {
//      system = ActorSystem("TestSystem", ConfigFactory.load().getConfig(Constants.defaultConfigSectionName))
//      withFixture(test.toNoArgTest(new Fixture(system))) // "loan" the fixture to the test
//    } finally {
//      TestKit.shutdownActorSystem(system)
//      Thread.sleep(1000)
//    }
//  }
//
//  "client executor" should {
//    "notify the client that server is ready" in { testkit ⇒
//      testkit.probe.expectMsg(1.seconds, ServerReady)
//    }
//
//    "successfuly execute a job request via a plugin" in { testkit ⇒ new Fixture(testkit, "client2") {
//
//    }
//    }
//  }
//
//  class Fixture(system: ActorSystem) extends TestKit(system) with ImplicitSender {
//
//    val probe = self
//
//    val executorService = new ExecutorService("TestService") {
//      // Run it locally in Spark
//      override val configurer: Configurer = new LocalConfigurer("TestService", None)
//    }
//
//    executorService.start(system, s"/user/$clientName")
//
//    val clientActor = system.actorOf(Props(new Actor {
//      override def receive = {
//        case ServerReady ⇒ testkit.self forward ServerReady
//      }
//    }), clientName)
//  }
//
//  override def afterAll {
//    TestKit.shutdownActorSystem(system)
//  }
//}
