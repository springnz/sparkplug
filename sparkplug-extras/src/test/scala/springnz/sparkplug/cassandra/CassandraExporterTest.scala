package springnz.sparkplug.cassandra

import java.time.{ LocalDate, LocalDateTime }

import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.scalatest._
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.testkit.SimpleTestContext
import springnz.sparkplug.util.{ DateTimeUtil, Logging }

case class TestRow(id: Int, localDate: LocalDate, localDateTime: LocalDateTime)

class CassandraExporterTest extends WordSpec with ShouldMatchers with Logging {

  "CassandraExporter" should {

    "handle Java 8 timestamps and select rows with a given LocalDate" ignore new SimpleTestContext("CassandraExporterTest") {
      import DateTimeUtil._

      val keySpace = ConfigFactory.load("sparkplug").getString("cassandra.test-keyspace")
      val table = "cassandra_exporter_test"

      val createQuery =
        s"""CREATE TABLE IF NOT EXISTS $keySpace.$table (
             |  id int,
             |  local_date timestamp,
             |  local_date_time timestamp,
             |  PRIMARY KEY (id, local_date)
             |);""".stripMargin

      // Given an RDD of rows

      // The following is persisted in Cassandra as "2015-10-21 11:00:00+0000"
      val localDate = isoDateToLocalDate("2015-10-22")
      val localDateTime = isoDateTimeToLocalDateTime("2015-10-22 13:01:02")

      val rows = List(
        TestRow(1, localDate.minusDays(1), localDateTime.minusDays(1)),
        TestRow(2, localDate, localDateTime),
        TestRow(3, localDate.plusDays(1), localDateTime.plusDays(1)))
      val testRDDSource: SparkOperation[RDD[TestRow]] = SparkOperation { _.parallelize(rows) }

      // Export the rows to Cassandra

      val exporter = CassandraExporter[TestRow](testRDDSource, keySpace, table, List(createQuery))

      val dropTableThenExport = for {
        _ ← CassandraUtil.dropTable(keySpace, table)
        result ← exporter
        (rdd, exportResult) = result
      } yield {
        (rdd.collect(), exportResult)
      }

      val (_, exportResult) = execute(dropTableThenExport).get
      exportResult.isSuccess shouldBe true

      // Select the row for a given LocalDate

      val cassandraRDDSource = SparkOperation { ctx ⇒
        ctx.cassandraTable[TestRow](keySpace, table).where("local_date = ?", localDate)
      }
      val resultSet = execute(cassandraRDDSource.map(_.collect())).get

      // Load the result set into an array and verify

      resultSet.length shouldBe 1
      resultSet.head.id shouldBe 2
      resultSet.head.localDate shouldBe localDate
      resultSet.head.localDateTime shouldBe localDateTime
    }
  }
}
