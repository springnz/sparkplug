package springnz.sparkplug.examples

import org.scalatest.{ ShouldMatchers, WordSpec }
import springnz.sparkplug.testkit.SimpleTestContext
import springnz.sparkplug.util.Logging

class LetterCountTests extends WordSpec with ShouldMatchers with Logging {
  "letter count" should {
    "return the count of a and b" in new SimpleTestContext("LetterCount") {

      val (a, b) = execute((new LetterCount)()).get
      a should be(2)
      b should be(2)
    }
  }

  "letter count (function style)" should {
    "return the count of a and b" in new SimpleTestContext("LetterCount") {

      val (a, b) = execute(LetterCountFunctionStyle()).get
      a should be(2)
      b should be(2)
    }
  }
}

