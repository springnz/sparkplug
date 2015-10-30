package springnz.sparkplug

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.testkit.{ ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest._
import springnz.sparkplug.core.{ Configurer, LocalConfigurer }
import springnz.sparkplug.executor.MessageTypes.{ JobRequest, JobSuccess, ServerReady }
import springnz.sparkplug.executor.{ Constants, ExecutorService }

import scala.concurrent.duration._

class ExecutorServiceTests(_system: ActorSystem)
    extends TestKit(_system) with ImplicitSender with WordSpecLike with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestSystem", ConfigFactory.load().getConfig(Constants.defaultConfigSectionName)))

  "notify the client that server is ready" in new Fixture(self, "client1") {
    expectMsg(1.seconds, ServerReady)
  }

  "successfuly execute a job request" in new Fixture(self, "client2") {
    val requestBroker = system.actorSelection(s"/user/${Constants.brokerActorName}")
    val request = JobRequest("springnz.sparkplug.examples.LetterCountPlugin", None)
    requestBroker ! request
    expectMsg[JobSuccess](6.seconds, JobSuccess(request, (2, 2)))
  }

  class Fixture(probe: ActorRef, clientName: String) {

    val executorService = new ExecutorService {
      // Run it locally in Spark
      override val configurer: Configurer = new LocalConfigurer("ExecutorService", None)
    }

    executorService.start(system, s"/user/$clientName")

    val clientActor = system.actorOf(Props(new Actor {
      override def receive = {
        case ServerReady ⇒ probe forward ServerReady
      }
    }), clientName)
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

}