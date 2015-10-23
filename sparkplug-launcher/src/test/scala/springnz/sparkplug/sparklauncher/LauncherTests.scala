package springnz.sparkplug.sparklauncher

import springnz.util.Logging
import org.scalatest.{ ShouldMatchers, WordSpec }

import scala.concurrent.Await
import scala.concurrent.duration._

class LauncherTests extends WordSpec with ShouldMatchers with Logging {

  "simple spark launch" should {
    "launch spark letter count process (without Akka)" ignore {
      val launcher = Launcher.apply(
        "",
        "target/pack/lib/spark-lib_2.11-0.2.4-SNAPSHOT.jar",
        "springnz.sparkplug.examples.LetterCount",
        "target/pack/lib")
      val result = Await.result(launcher.get, 60 seconds)

      // this is not complete
    }
  }
}

