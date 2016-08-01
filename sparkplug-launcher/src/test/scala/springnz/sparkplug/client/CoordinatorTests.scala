package springnz.sparkplug.client

import akka.actor.{ ExtendedActorSystem, ActorRef, ActorSystem }
import akka.pattern.ask
import akka.testkit.{ ImplicitSender, TestKit }
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest._
import springnz.sparkplug.executor.MessageTypes.{ JobFailure, JobRequest, JobSuccess, ShutDown }

import scala.concurrent.Await
import scala.concurrent.duration._

import scala.collection.JavaConverters._

class CoordinatorTests(_system: ActorSystem)
    extends TestKit(_system) with ImplicitSender with WordSpecLike with BeforeAndAfterAll with Matchers {

  def this() = this(ActorSystem(Constants.actorSystemName, ConfigFactory.parseMap(Map(
    "akka.remote.netty.tcp.port" -> new Integer(0)).asJava).withFallback(ClientExecutor.defaultClientAkkaConfig)))

  var coordinator: ActorRef = null

  "client coordinator" should {

    "successfuly execute a job request" in {
      val request = JobRequest("springnz.sparkplug.executor.LetterCountPlugin", None)
      coordinator ! request
      expectMsg[JobSuccess](30.seconds, JobSuccess(request, (2, 2)))
    }

    "successfuly execute a job request after a failure" in {
      val invalidRequest = JobRequest("springnz.sparkplug.executor.InvalidClass", None)
      coordinator ! invalidRequest
      expectMsgType[JobFailure](30.seconds)
      val goodRequest = JobRequest("springnz.sparkplug.executor.LetterCountPlugin", None)
      coordinator ! goodRequest
      expectMsg[JobSuccess](30.seconds, JobSuccess(goodRequest, (2, 2)))
    }

    "work with the ask pattern as well" in {
      implicit val timeout = Timeout(30.seconds)
      val request = JobRequest("springnz.sparkplug.executor.LetterCountPlugin", None)
      val replyFuture = coordinator ? request
      val result = Await.result(replyFuture, 30.seconds)
      result shouldBe JobSuccess(request, (2, 2))
    }

  }

  override def beforeAll {
    val configSection = s"sparkplug.${springnz.sparkplug.executor.Constants.defaultAkkaRemoteConfigSection}"
    val port = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress.port.get
    val akkaClientConfig = ConfigFactory.parseMap(Map(
      "akka.remote.netty.tcp.port" -> new Integer(port)).asJava).withFallback(ClientExecutor.defaultClientAkkaConfig)
    coordinator = system.actorOf(Coordinator.props(None,
      akkaRemoteConfig = Some(ConfigFactory.load.getConfig(configSection)),
      akkaClientConfig = akkaClientConfig), "TestCoordinator")
  }

  override def afterAll {
    system.actorSelection(s"/user/TestCoordinator") ! ShutDown
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

}

