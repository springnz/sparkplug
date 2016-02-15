package springnz.sparkplug.util

import java.time.{ Instant ⇒ JInstant, _ }
import java.util.Date

import org.joda.time.DateTime

import scala.language.implicitConversions

private[sparkplug] object DatePimpers {

  class OrderedOffsetDateTime(offsetDateTime: OffsetDateTime) extends Ordered[OffsetDateTime] {
    def compare(that: OffsetDateTime): Int = offsetDateTime.compareTo(that)
  }

  implicit def orderedOffsetDateTime(offsetDateTime: OffsetDateTime): OrderedOffsetDateTime =
    new OrderedOffsetDateTime(offsetDateTime)

  class OrderedLocalDateTime(localDateTime: LocalDateTime) extends Ordered[LocalDateTime] {
    def compare(that: LocalDateTime): Int = localDateTime.compareTo(that)
  }

  implicit def orderedLocalDateTime(localDateTime: LocalDateTime): OrderedLocalDateTime =
    new OrderedLocalDateTime(localDateTime)

  class OrderedLocalDate(localDate: LocalDate) extends Ordered[LocalDate] {
    def compare(that: LocalDate): Int = localDate.compareTo(that)
  }

  implicit def orderedLocalDate(localDate: LocalDate): OrderedLocalDate =
    new OrderedLocalDate(localDate)

  implicit class OffsetDateTimePimper(offsetDateTime: OffsetDateTime) {
    def toLegacyDate: Date = Date.from(offsetDateTime.toInstant)
    def toJodaTime: DateTime = DateTime.parse(offsetDateTime.toString)
  }

  implicit class ZonedDateTimePimper(zonedDateTime: ZonedDateTime) {
    def toLegacyDate: Date = Date.from(zonedDateTime.toInstant)
  }

  implicit class LocalDateTimePimper(localDateTime: LocalDateTime) {
    def toNZLegacyDate: Date = Date.from(localDateTime.atZone(DateTimeUtil.NZTimeZone).toInstant)
  }

  implicit class LocalDatePimper(localDate: LocalDate) {
    def toNZLegacyDate: Date = Date.from(localDate.atStartOfDay().atZone(DateTimeUtil.NZTimeZone).toInstant)
  }

  implicit class LegacyDatePimper(date: Date) {
    def toUtcOffsetDateTime: OffsetDateTime = OffsetDateTime.ofInstant(date.toInstant, DateTimeUtil.UTCTimeZone)
    def toUtcLocalDateTime: LocalDateTime = LocalDateTime.ofInstant(date.toInstant, DateTimeUtil.UTCTimeZone)
    def toNZLocalDateTime: LocalDateTime = LocalDateTime.ofInstant(date.toInstant, DateTimeUtil.NZTimeZone)
    def toNZLocalDate: LocalDate = toNZLocalDateTime.toLocalDate
  }

  implicit class JodaTimePimper(dateTime: DateTime) {
    def toUtcOffsetDateTime: OffsetDateTime = {
      val jInstant = JInstant.ofEpochMilli(dateTime.toInstant.getMillis)
      OffsetDateTime.ofInstant(jInstant, DateTimeUtil.UTCTimeZone)
    }
  }
}

