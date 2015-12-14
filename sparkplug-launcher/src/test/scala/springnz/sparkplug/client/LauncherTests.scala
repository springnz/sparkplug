package springnz.sparkplug.client

import better.files._
import org.scalatest._
import springnz.util.Logging

import scala.concurrent.Await
import scala.concurrent.duration._

class LauncherTests extends WordSpec with ShouldMatchers with Logging {

  "simple spark launch" should {
    "launch spark letter count process (without Akka)" in {
      val launcher = Launcher.launch("", root / System.getProperty("user.dir") / "target/pack/lib", "sparkplug-examples_2.11-0.2.6-SNAPSHOT.jar", "springnz.sparkplug.examples.LetterCount")
      val result = Await.result(launcher.get, 20 seconds)

      // this way of executing does not return anything
      result shouldBe (())
    }
  }
}

