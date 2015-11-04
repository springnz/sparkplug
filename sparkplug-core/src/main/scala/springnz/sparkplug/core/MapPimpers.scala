package springnz.sparkplug.core

import scala.annotation.tailrec

object MapPimpers {
  implicit class MapOptionClass[A, B](map: Map[A, Option[B]]) {

    @deprecated("Use Pimpers.removeNones in util-lib")
    def filterSomes() = map filter {
      case (_, ob) ⇒ ob.isDefined
    }
  }

  implicit class MapPimperClass[A, B](map: Map[A, B]) {
    @deprecated("Use Pimpers.mapValuesRemoveNones in util-lib")
    def mapValuesRemoveNones[C](f: B ⇒ Option[C]): Map[A, C] =
      map.mapValues(f)
        .filter { case (k, ov) ⇒ ov.isDefined }
        .map {
          case (k, Some(v)) ⇒ (k, v)
          case _            ⇒ throw new Exception // should never happen - just there to suppress warning
        }

    @deprecated("Use Pimpers.getFirst in util-lib")
    @tailrec
    final def getFirst(seqA: A*): Option[B] = seqA.headOption match {
      case None ⇒ None
      case Some(head) ⇒ map.get(head) match {
        case None  ⇒ getFirst(seqA.tail: _*)
        case value ⇒ value
      }
    }

  }
}
