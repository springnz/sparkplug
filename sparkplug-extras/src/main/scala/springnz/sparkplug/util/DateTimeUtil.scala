package springnz.sparkplug.util

import java.time.format.DateTimeFormatter
import java.time.{ Instant ⇒ JInstant, _ }

import org.joda.time.DateTime

private[sparkplug] object DateTimeUtil {

  val UTCTimeZone: ZoneId = ZoneId.of("UTC")
  val NZTimeZone: ZoneId = ZoneId.of("Pacific/Auckland")

  val isoDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val isoDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def utcOffsetDateTime: OffsetDateTime = OffsetDateTime.now(UTCTimeZone)

  def nzLocalDateTime: LocalDateTime = OffsetDateTime.now(NZTimeZone).toLocalDateTime

  def utcZonedDateTime: ZonedDateTime = ZonedDateTime.now(UTCTimeZone)

  def toIsoDate(localDate: LocalDate): String = localDate.format(isoDateFormatter)

  def toIsoDate(zonedDateTime: ZonedDateTime): String = zonedDateTime.format(isoDateFormatter)

  def toIsoDate(offsetDateTime: OffsetDateTime): String = offsetDateTime.format(isoDateFormatter)

  def toIsoDate(dateTime: DateTime): String = dateTime.toString("yyyy-MM-dd")

  def toIsoDateTime(localDate: LocalDate): String = localDate.format(isoDateTimeFormatter)

  def toIsoDateTime(localDateTime: LocalDateTime): String = localDateTime.format(isoDateTimeFormatter)

  def toIsoDateTime(zonedDateTime: ZonedDateTime): String = zonedDateTime.format(isoDateTimeFormatter)

  def toIsoDateTime(offsetDateTime: OffsetDateTime): String = offsetDateTime.format(isoDateTimeFormatter)

  def toIsoDateTime(dateTime: DateTime): String = dateTime.toString("yyyy-MM-dd hh:mm:ss")

  def isoDateToLocalDate(isoDate: String): LocalDate = LocalDate.parse(isoDate, isoDateFormatter)

  def isoDateTimeToLocalDateTime(isoDateTime: String): LocalDateTime = LocalDateTime.parse(isoDateTime, isoDateTimeFormatter)
}
