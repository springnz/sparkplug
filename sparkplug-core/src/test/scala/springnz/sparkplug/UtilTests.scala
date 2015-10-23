package springnz.sparkplug
import springnz.sparkplug.util.SerializeUtils._

import org.scalatest.{ ShouldMatchers, WordSpec }

case class TestObject(a: String, b: Int, c: Vector[String], d: List[Int])

class UtilTests extends WordSpec with ShouldMatchers {

  "serialise utils" should {
    "serialise and deserialise a local object" in {
      val testObject = TestObject("hello", 42, Vector("test", "array"), List(42, 108))
      val byteArray = serialize(testObject)
      val inflatedObject = deserialize[TestObject](byteArray, this.getClass.getClassLoader)

      inflatedObject should equal(testObject)
    }

    "serialise and deserialise a local object with its class loader" in {
      val testObject = TestObject("hello", 42, Vector("test", "array"), List(42, 108))
      val byteArray = serialize(testObject)
      val inflatedObject = deserialize[TestObject](byteArray, TestObject.getClass.getClassLoader)

      inflatedObject should equal(testObject)
    }

    "serialise and deserialise a local object with default class loader" in {
      val testObject = TestObject("hello", 42, Vector("test", "array"), List(42, 108))
      val byteArray = serialize(testObject)
      val inflatedObject = deserialize[TestObject](byteArray)

      inflatedObject should equal(testObject)
    }
  }
}
