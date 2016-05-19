package springnz.sparkplug.client

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ ImplicitSender, TestKit }
import org.scalatest._
import springnz.sparkplug.executor.MessageTypes.{CancelAllJobs, JobRequest, JobSuccess, ShutDown}

import scala.concurrent.duration._

class CoordinatorCleanupTests(_system: ActorSystem)
    extends TestKit(_system) with ImplicitSender with WordSpecLike with BeforeAndAfterAll with Matchers {

  def this() = this(ActorSystem(Constants.actorSystemName, ClientExecutor.defaultClientAkkaConfig))

  var coordinator: ActorRef = null

  "client coordinator" should {

    "successfuly execute a job request" in {
      val request = JobRequest("springnz.sparkplug.examples.WaitPlugin", None)
      coordinator ! request
      expectMsgType[JobSuccess](30.seconds)
    }

    "cancel all job requests" in {
      val request = JobRequest("springnz.sparkplug.examples.WaitPlugin", None)
      coordinator ! request
      Thread.sleep(500)
      coordinator ! CancelAllJobs
      Thread.sleep(500)
      expectMsgType[JobSuccess](30.seconds)
    }

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
