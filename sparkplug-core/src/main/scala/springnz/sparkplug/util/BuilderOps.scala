package springnz.sparkplug.util

private[sparkplug] object BuilderOps {
  implicit class ReqBuilderOps[A](builder: A) {
    def setIfSome[B](value: Option[B])(setMethod: (A, B) ⇒ A) = value match {
      case None             ⇒ builder
      case Some(innerValue) ⇒ setMethod(builder, innerValue)
    }

    def setIfNonEmpty(value: String)(setMethod: (A, String) ⇒ A) =
      if (value.isEmpty) builder else setMethod(builder, value)

    def setFoldLeft[B](iterable: Iterable[B])(setMethod: (A, B) ⇒ A): A = {
      iterable.foldLeft(builder)(setMethod)
    }
  }
}
