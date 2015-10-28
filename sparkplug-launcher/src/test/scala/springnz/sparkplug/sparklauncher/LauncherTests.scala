package springnz.sparkplug.client

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, ImplicitSender }
import com.typesafe.config.ConfigFactory
import org.scalatest._
import springnz.util.Logging

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class LauncherTests extends WordSpec with ShouldMatchers with Logging {

  "simple spark launch" should {
    "launch spark letter count process (without Akka)" in {
      val launcher = Launcher.apply("", "target/pack/lib", "target/pack/lib/spark-lib_2.11-0.2.6-SNAPSHOT.jar", "springnz.sparkplug.examples.LetterCount")
      val result = Await.result(launcher.get, 60 seconds)

      // this way of executing does not return anything
      result shouldBe (())
    }
  }
}

trait ClientExecutableFixture extends BeforeAndAfterEach { this: Suite ⇒

  var executor: ClientExecutor = null

  override def beforeEach() {
    try executor = ClientExecutor.create()
    finally super.beforeEach()
  }

  override def afterEach() {
    try super.afterEach()
    finally {
      executor.shutDown()
      Thread.sleep(1000)
      // it seems to need to be give a chance to clean up (I think shutdown happens asnychronously)
    }
  }
}

class ClientExecutorTests extends WordSpec with ShouldMatchers with Logging with ClientExecutableFixture with Inspectors {
  implicit val ec = scala.concurrent.ExecutionContext.global

  "client executor" should {
    "Calculate a single job" in {
      val future = executor.execute[Any]("springnz.sparkplug.examples.LetterCountPlugin", None)
      val result = Await.result(future, 120 seconds)
      result shouldBe ((2, 2))

    }

    "Handle an error in a job request" in {
      val future = executor.execute[Any]("springnz.sparkplug.examples.InvalidPluginName", None)
      intercept[ClassNotFoundException] {
        Await.result(future, 120 seconds)
      }
    }

    "Calculate a sequence of job requests in parallel" in {
      val futures: List[Future[Any]] = List.fill(10) { executor.execute[Any]("springnz.sparkplug.examples.LetterCountPlugin", None) }

      implicit val ec = scala.concurrent.ExecutionContext.global
      val sequence: Future[List[Any]] = Future.sequence(futures)
      val results = Await.result(sequence, 120 seconds)

      // this way of executing does not return anything
      results shouldBe a[List[_]]

      results.length shouldBe 10

      forAll(results) {
        result ⇒ result shouldBe ((2, 2))
      }
    }
  }
}

class CoordinatorTests(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with BeforeAndAfterAll {

  def this() = this(ActorSystem("TestSystem", ConfigFactory.load().getConfig("sparkPlugAkkaExecutorService")))

  implicit val ec = scala.concurrent.ExecutionContext.global

  "coordinator" should {
    "Calculate a single job" in {
    }
  }
}