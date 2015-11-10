package springnz.sparkplug

import org.apache.spark.SparkContext
import org.scalatest.{ ShouldMatchers, WordSpec }
import springnz.sparkplug.core.SparkOperation._
import springnz.sparkplug.core._
import springnz.util.Logging

import scalaz.syntax.bind._

class OperationExamples extends WordSpec with ShouldMatchers with Logging {
  trait DummyExecutor extends ExecutorU {
    override protected def configurer: Configurer = ??? // we don't care about configurations in these tests
    override def execute[A](operation: SparkOperation[A]): A = operation.run(null) // just don't use context in these tests
  }

  "Simple springnz.sparkplug.examples" should {
    "operation as function" in new DummyExecutor {
      val operationInt = SparkOperation { ctx ⇒ 123 }
      val operationString = SparkOperation { ctx ⇒ "mama" }

      val operationWithDependencies =
        (first: Int, second: String) ⇒ SparkOperation { ctx ⇒ first.toString + second }

      val merged = for {
        i ← operationInt
        s ← operationString
        together ← operationWithDependencies(i, s)
      } yield together

      execute(merged) shouldBe "123mama"
    }

    "operation as class" in new DummyExecutor {
      val operationInt = SparkOperation { ctx ⇒ 123 }
      val operationString = SparkOperation { ctx ⇒ "mama" }

      case class SomeMergedGuy(first: Int, second: String) {
        def apply() = SparkOperation { ctx ⇒ first.toString + second }
      }

      val merged = for {
        i ← operationInt
        s ← operationString
        together ← SomeMergedGuy(i, s)()
      } yield together

      execute(merged) shouldBe "123mama"
    }

    "operation as wrapper" in new DummyExecutor {
      class WrappedOperation {
        def op1 = SparkOperation { ctx ⇒ 123 }
        def apply() = op1.map(_ * 2)
      }

      val operationInt = (new WrappedOperation)()
      execute(operationInt) shouldBe 246
    }

    "operation as function instance" in new DummyExecutor {
      class FunctionOperation extends (() ⇒ SparkOperation[Int]) {
        def apply() = SparkOperation { ctx ⇒ 123 }
      }

      val operationInt = new FunctionOperation
      execute(operationInt()) shouldBe 123
    }

    "longer computation" in new DummyExecutor {
      case class Provider1(first: Int, second: Int) {
        def apply() = SparkOperation { ctx ⇒ first + second }
      }

      case class Provider2(first: String, second: String) {
        def apply() = SparkOperation { ctx ⇒ first.length + second.length }
      }

      val myProvider = for {
        r1 ← Provider1(1, 1)()
        r2 ← Provider2("a", "bcd")()
        summed ← Provider1(r1, r2)()
        // these could go inside yield
        a = 123
        b = 23
      } yield a - b + summed

      execute(myProvider) shouldBe (100 + 2 + 4)
    }

    "simple fill" in new DummyExecutor {
      case class Provider(first: Int, second: Int) {
        def apply() = SparkOperation { ctx ⇒ first + second }
      }

      val quiteFilled = (second: Int) ⇒ for {
        p ← Provider(500, second)()
      } yield p

      execute(quiteFilled(250)) shouldBe 750
    }

    "bit more complex fill" in new DummyExecutor {
      case class Provider(first: Int, second: Int) {
        def apply() = SparkOperation { ctx ⇒ first + second }
      }

      val harder = (first: Int, second: Int) ⇒ for {
        p1 ← Provider(500, second)()
        p2 ← Provider(first, 100)()
      } yield p1 + p2

      execute(harder(-500, -100)) shouldBe 0

      val another = for {
        x ← harder(-500, -100)
        y ← Provider(3, 7)()
      } yield x + y

      execute(another) shouldBe 10
    }

    "generic operation should compile" in new DummyExecutor {
      case class Generic[A]() {
        def apply() = SparkOperation { (ctx: SparkContext) ⇒ 555 }
      }

      val z = new Generic[Int]()
      z().map(_.toString)
      z().flatMap(null)

      for { x ← Generic[Int]()() } yield ()
    }
  }

  "Advanced springnz.sparkplug.examples" should {
    "Partial graph fill" in new DummyExecutor {
      val IProduceInt = SparkOperation { ctx ⇒ 666 }

      case class ITake3Arguments(first: Int, second: Boolean, third: String) {
        def apply() = SparkOperation { (ctx: SparkContext) ⇒ first.toString + second.toString + third.toString }
      }

      // also could be another case class
      val firstHoleFilled = (second: Boolean) ⇒ for {
        i ← IProduceInt
        m ← ITake3Arguments(i, second, "mama")()
      } yield m

      execute(firstHoleFilled(false)) shouldBe "666falsemama"
    }

    "Mutating operation (prehaps for that dynamic case on whiteboard?)" in new DummyExecutor {
      val IProduceInt = SparkOperation { ctx ⇒ 10 }

      import scala.collection.mutable._
      class DynamicOperation(startSum: Int, xs: Buffer[String]) {
        def apply() = SparkOperation { ctx ⇒
          xs.foldRight(startSum)(_.length + _) // or I do some join on some other tables or something
        }
      }

      val xs = Buffer.empty[String]
      val operation = new DynamicOperation(0, xs)

      val dynamicOperation = for {
        i ← IProduceInt
        r ← (new DynamicOperation(i, xs))()
      } yield r

      execute(dynamicOperation) shouldBe 10 + 0
      xs += "test"
      execute(dynamicOperation) shouldBe 10 + 4
    }

  }

  "Tutorials" should {
    // if you miss/don't know something either come to me (marek kadek)
    // or feel free to expand.

    // Monad allows us to chain computation.
    // - you can use value from operation1 in operation2, if it was needed, i.e. ...b <- operation2(a)
    // - first problem stops the computation. i.e. if operation2 died (exception), operation3 would not run
    // - hence is not parallelizable (because next steps may depend on previous)
    "Monad" in new DummyExecutor {
      val operation1 = SparkOperation { ctx ⇒ 1 }
      val operation2 = SparkOperation { ctx ⇒ 2 }
      val operation3 = SparkOperation { ctx ⇒ 3 }

      // we don't use next stuff we from previous computations... it's so that I can show same example with
      // applicative
      val sumUp = for {
        a ← operation1
        b ← operation2
        c ← operation3
      } yield a + b + c

      execute(sumUp) shouldBe (1 + 2 + 3)
    }

    // Applicatives are less common but just as useful as monads.
    // - the operations are independent
    // - all computations run, i.e. if operation2 dies (exception), operation 1 and 3 would run
    // - hence is parallelizable - there is no dependency on context (no `previous step`)
    "Applicative" in new DummyExecutor {
      val operation1 = SparkOperation { ctx ⇒ 1 }
      val operation2 = SparkOperation { ctx ⇒ 2 }
      val operation3 = SparkOperation { ctx ⇒ 3 }

      val sumUp = SparkOperation.monad.apply3(operation1, operation2, operation3) {
        case (a, b, c) ⇒ a + b + c
      }

      // more compact, scalaZ style
      val sumUpZ = (operation1 ⊛ operation2 ⊛ operation3) { case (a, b, c) ⇒ a + b + c }

      execute(sumUp) shouldBe (1 + 2 + 3)
      execute(sumUpZ) shouldBe (1 + 2 + 3)
    }

    // So you have list of operations, and you want such operation, that if executed, list
    // of results is returned.
    // Sequence is special case of more general, Traverse
    "Sequence" in new DummyExecutor {
      val operation1 = SparkOperation { ctx ⇒ 1 }
      val operation2 = SparkOperation { ctx ⇒ 2 }
      val operation3 = SparkOperation { ctx ⇒ 3 }

      val list = List(operation1, operation2, operation3)

      import scalaz.std.list.listInstance // List is indeed traversable
      val swap: SparkOperation[List[Int]] = SparkOperation.monad.sequence(list)

      execute(swap) should be(Seq(1, 2, 3))
    }

    "sequence and apply work for derived classes" in new DummyExecutor {
      case class SparkOperationInt(i: Int) {
        def apply() = SparkOperation { ctx ⇒ i }
      }

      // note that the first argument needs to be applied as a SparkOperation[_]
      // for the methods below to work
      // if the line below were
      // val operation1 = SparkOperationInt(1)
      // it fails to build
      val operation1 = SparkOperationInt(1)()
      val operation2 = SparkOperationInt(2)()
      val operation3 = SparkOperationInt(3)()

      val sumUpZ = (operation1 ⊛ operation2 ⊛ operation3) { case (a, b, c) ⇒ a + b + c }

      val list = List(operation1, operation2, operation3)

      import scalaz.std.list.listInstance
      val swap = SparkOperation.monad.sequence(list)

      execute(swap) should be(Seq(1, 2, 3))
      execute(sumUpZ) shouldBe (1 + 2 + 3)
    }

    "sequence and apply work for function instances" in new DummyExecutor {
      object SparkOperationFunction extends (Int ⇒ SparkOperation[Int]) {
        override def apply(i: Int) = SparkOperation { ctx ⇒ i }
      }

      val operation1 = SparkOperationFunction(1)
      val operation2 = SparkOperationFunction(2)
      val operation3 = SparkOperationFunction(3)

      val sumUpZ = (operation1 ⊛ operation2 ⊛ operation3) { case (a, b, c) ⇒ a + b + c }

      val list = List(operation1, operation2, operation3)

      import scalaz.std.list.listInstance
      val swap = SparkOperation.monad.sequence(list)

      execute(swap) should be(Seq(1, 2, 3))
      execute(sumUpZ) shouldBe (1 + 2 + 3)
    }

  }
}
