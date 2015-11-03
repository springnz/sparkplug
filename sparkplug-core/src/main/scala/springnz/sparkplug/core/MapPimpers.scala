package springnz.sparkplug.core

object MapPimpers {
  implicit class MapOptionClass[A, B](map: Map[A, Option[B]]) {
    def filterSomes() = map filter {
      case (_, ob) ⇒ ob.isDefined
    }
  }

  implicit class MapPimperClass[A, B](map: Map[A, B]) {
    def mapValuesRemoveNones[C](f: B ⇒ Option[C]): Map[A, C] =
      map.mapValues(f)
        .filter { case (k, ov) ⇒ ov.isDefined }
        .map {
          case (k, Some(v)) ⇒ (k, v)
          case _            ⇒ throw new Exception // should never happen - just there to suppress warning
        }

    def getFirst(seqA: A*): Option[B] = seqA.length match {
      case 0 ⇒ None
      case _ ⇒ map.get(seqA.head) match {
        case None  ⇒ getFirst(seqA.tail: _*)
        case value ⇒ value
      }

    }
  }
}

class LetterCountPlugin  {
  def apply(input: Any) = ???
}