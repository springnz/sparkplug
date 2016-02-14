package springnz.sparkplug.cassandra

import java.time.{ LocalDate, LocalDateTime }
import java.util.Date

import com.datastax.spark.connector.types._
import springnz.sparkplug.util.DatePimpers._
import springnz.sparkplug.util.Logging

import scala.reflect.runtime.universe._

object CustomTypeConverters extends Logging {

  private var isTimeConvertersRegistered = false

  object LegacyDateToLocalDateConverter extends TypeConverter[LocalDate] {
    def targetTypeTag = typeTag[LocalDate]
    def convertPF = {
      case date: Date ⇒
        val localDate = date.toNZLocalDate
        log.trace(s"legacyDate=$date -> localDate=$localDate")
        localDate
    }
  }

  object LocalDateToLegacyDateConverter extends TypeConverter[Date] {
    def targetTypeTag = typeTag[Date]
    def convertPF = {
      case localDate: LocalDate ⇒
        val legacyDate = localDate.toNZLegacyDate
        log.trace(s"localDate=$localDate -> legacyDate=$legacyDate")
        legacyDate
    }
  }

  object LegacyDateToLocalDateTimeConverter extends TypeConverter[LocalDateTime] {
    def targetTypeTag = typeTag[LocalDateTime]
    def convertPF = {
      case date: Date ⇒
        val localDateTime = date.toNZLocalDateTime
        log.trace(s"legacyDate=$date -> localDateTime=$localDateTime")
        localDateTime
    }
  }

  object LocalDateTimeToLegacyDateConverter extends TypeConverter[Date] {
    def targetTypeTag = typeTag[Date]
    def convertPF = {
      case localDateTime: LocalDateTime ⇒
        val legacyDate = localDateTime.toNZLegacyDate
        log.trace(s"localDateTime=$localDateTime -> legacyDate=$legacyDate")
        legacyDate
    }
  }

  def registerTimeConverters(): Unit =
    if (!isTimeConvertersRegistered) {
      log.info("Registering Java 8 Time type converters for Cassandra Spark connector")
      TypeConverter.registerConverter(LegacyDateToLocalDateConverter)
      TypeConverter.registerConverter(LegacyDateToLocalDateTimeConverter)
      TypeConverter.registerConverter(LocalDateToLegacyDateConverter)
      TypeConverter.registerConverter(LocalDateTimeToLegacyDateConverter)
      isTimeConvertersRegistered = true
    }
}
