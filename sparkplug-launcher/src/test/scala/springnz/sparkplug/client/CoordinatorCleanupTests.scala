package springnz.sparkplug.client

import akka.actor.{ ActorRef, Props, ActorSystem }
import akka.testkit.{ ImplicitSender, TestActorRef, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest._
import springnz.sparkplug.executor.MessageTypes.{ JobRequest, JobSuccess, ShutDown }

import scala.concurrent.duration._

class CoordinatorCleanupTests(_system: ActorSystem)
    extends TestKit(_system) with ImplicitSender with WordSpecLike with BeforeAndAfterAll with Matchers {

  def this() = this(ActorSystem(Constants.actorSystemName, ConfigFactory.load().getConfig(Constants.defaultConfigSectionName)))

  var coordinator: ActorRef = null

  "client coordinator" should {

    "successfuly execute a job request" in {
      val request = JobRequest("springnz.sparkplug.examples.WaitPlugin", None)
      coordinator ! request
      expectMsgType[JobSuccess](30.seconds)
    }

    // TODO: work out a way to kill off the broker to test DeathWatch
  }

  override def beforeAll {
    coordinator = system.actorOf(Coordinator.props(None), "TestCoordinator")
  }

  override def afterAll {
    system.actorSelection(s"/user/TestCoordinator") ! ShutDown
    TestKit.shutdownActorSystem(system)
    Thread.sleep(1000)
  }

}
