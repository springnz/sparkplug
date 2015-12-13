package springnz.sparkplug.client

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest._
import springnz.sparkplug.executor.MessageTypes.{JobFailure, JobRequest, JobSuccess, ShutDown}

import scala.concurrent.Await
import scala.concurrent.duration._

class CoordinatorTests(_system: ActorSystem)
    extends TestKit(_system) with ImplicitSender with WordSpecLike with BeforeAndAfterAll with Matchers {

  def this() = this(ActorSystem(Constants.actorSystemName, ConfigFactory.load().getConfig(Constants.defaultConfigSectionName)))

  var coordinator: ActorRef = null

  "client coordinator" should {

    "successfuly execute a job request" in {
      val request = JobRequest("springnz.sparkplug.examples.LetterCountPlugin", None)
      coordinator ! request
      expectMsg[JobSuccess](20.seconds, JobSuccess(request, (2, 2)))
    }

    "successfuly execute a job request after a failure" in {
      val invalidRequest = JobRequest("springnz.sparkplug.examples.InvalidClass", None)
      coordinator ! invalidRequest
      expectMsgType[JobFailure](20.seconds)
      val goodRequest = JobRequest("springnz.sparkplug.examples.LetterCountPlugin", None)
      coordinator ! goodRequest
      expectMsg[JobSuccess](20.seconds, JobSuccess(goodRequest, (2, 2)))
    }

    "work with the ask pattern as well" in {
      implicit val timeout = Timeout(20 seconds)
      val request = JobRequest("springnz.sparkplug.examples.LetterCountPlugin", None)
      val replyFuture = coordinator ? request
      val result = Await.result(replyFuture, 20.seconds)
      result shouldBe JobSuccess(request, (2, 2))
    }

  }

  override def beforeAll {
    coordinator = TestActorRef(new Coordinator, "TestCoordinator")
  }

  override def afterAll {
    system.actorSelection(s"/user/TestCoordinator") ! ShutDown
    TestKit.shutdownActorSystem(system)
    Thread.sleep(1000)
  }

}


