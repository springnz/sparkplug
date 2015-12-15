package springnz.sparkplug.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scalaz._

sealed trait SparkOperation[+A] {
  def run(ctx: SparkContext): A

  // This enables out the box map / flatMap / for comprehension support
  // without needing to import scalaz.syntax.bind._
  def map[B](f: A ⇒ B): SparkOperation[B] = SparkOperation.monad.map(this)(f)
  def flatMap[B](f: A ⇒ SparkOperation[B]): SparkOperation[B] = SparkOperation.monad.bind(this)(f)
  def withFilter(p: A ⇒ Boolean): SparkOperation[A] = this // this enables pattern matching in for-comprehensions
}

object SparkOperation {
  def apply[A](f: SparkContext ⇒ A): SparkOperation[A] = new SparkOperation[A] {
    override def run(ctx: SparkContext): A = f(ctx)
  }

  implicit val monad = new Monad[SparkOperation] {
    override def bind[A, B](fa: SparkOperation[A])(f: A ⇒ SparkOperation[B]): SparkOperation[B] =
      SparkOperation(ctx ⇒ f(fa.run(ctx)).run(ctx))

    override def point[A](a: ⇒ A): SparkOperation[A] = SparkOperation(_ ⇒ a)
  }
}

trait SparkPlugin {
  def apply(input: Option[Any]): SparkOperation[Any]
}

