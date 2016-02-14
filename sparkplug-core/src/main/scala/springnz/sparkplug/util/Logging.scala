package springnz.sparkplug.util

import com.typesafe.scalalogging.{ LazyLogging, Logger }

private[sparkplug] trait Logging extends LazyLogging {
  implicit lazy val log: Logger = logger
}
