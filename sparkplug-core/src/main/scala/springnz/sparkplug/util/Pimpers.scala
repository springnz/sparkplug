package springnz.sparkplug.util

import com.typesafe.scalalogging.Logger

import scala.annotation.tailrec
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

private[sparkplug] object Pimpers {

  implicit class TryPimper[A](t: Try[A]) {
    def withErrorLog(msg: String)(implicit log: Logger): Try[A] =
      t.recoverWith {
        case e ⇒
          log.error(msg, e)
          Failure(e)
      }

    def withFinally[T](block: ⇒ T): Try[A] = {
      block
      t
    }
  }

  implicit class FuturePimper[T](f: Future[T]) {
    def withErrorLog(msg: String)(implicit log: Logger, ec: ExecutionContext): Future[T] = {
      f.onFailure {
        case e ⇒ log.error(msg, e)
      }
      f
    }

    def followedBy[S](f2: Future[S])(implicit ec: ExecutionContext) =
      f.fallbackTo(Future.successful(())).flatMap(_ ⇒ f2)
  }

  implicit class OptionPimper[T](o: Option[T]) {
    def withErrorLog(msg: String)(implicit log: Logger): Option[T] =
      o.orElse {
        log.error(msg)
        None
      }
  }

  implicit class MapOptionPimper[A, B](map: Map[A, Option[B]]) {
    def removeNones(): Map[A, B] = map.filter { case (_, ob) ⇒ ob.isDefined }.mapValues(_.get)
  }

  implicit class MapPimper[A, B](map: Map[A, B]) {
    def mapValuesRemoveNones[C](f: B ⇒ Option[C]): Map[A, C] =
      map.mapValues(f)
        .filter { case (k, ov) ⇒ ov.isDefined }
        .map {
          case (k, Some(v)) ⇒ (k, v)
          case _            ⇒ throw new Exception // should never happen - just there to suppress warning
        }

    @tailrec
    final def getFirst(seqA: A*): Option[B] = seqA.headOption match {
      case None ⇒ None
      case Some(head) ⇒ map.get(head) match {
        case None  ⇒ getFirst(seqA.tail: _*)
        case value ⇒ value
      }
    }
  }

  implicit class TraversableTryPimper[A](travTry: Traversable[Try[A]]) {
    def sequence(): Try[Traversable[A]] = {
      val empty: Try[Traversable[A]] = Success(Traversable.empty)

      travTry.foldRight(empty) {
        case (triedValue, triedResults) ⇒
          for {
            nextResult ← triedValue
            previousResults ← triedResults
          } yield Traversable(nextResult) ++ previousResults
      }
    }
  }

  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
    (new java.util.Properties /: map) { case (props, (k, v)) ⇒ props.put(k, v); props }
  }
}
