package springnz.sparkplug.util

private[sparkplug] object BuilderOps {
  implicit class ReqBuilderOps[A](builder: A) {
    def setIfSome[B](value: Option[B], setMethod: (A, B) ⇒ A) = value match {
      case None             ⇒ builder
      case Some(innerValue) ⇒ setMethod(builder, innerValue)
    }
  }
}
