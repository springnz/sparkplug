package springnz.sparkplug.orientdb

import com.metreta.spark.orientdb.connector._
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import org.scalatest.{ ShouldMatchers, WordSpec }
import springnz.orientdb.session.ODBSession
import springnz.orientdb.test.ODBMemoryTest
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.orientdb.OrientRDDFetcherTest.Dictator
import springnz.sparkplug.testkit._

class OrientRDDFetcherTest extends WordSpec with ShouldMatchers with ODBMemoryTest {

  def dbName = "test"
  def classNames = List.empty

  "OrientRDDFetcher reads ODocuments from OrientDB" in new SimpleTestContext("") {

    val numDocs = 1000
    val className = "TestClass"

    ODBSession { implicit db ⇒
      def newDoc(id: Int) = {
        val doc = new ODocument(className)
        doc.field("id", id)
        doc.field("value", id)
        doc.save()
      }
      log.info(s"creating class $className ...")
      createClass(className).createProperty("id", OType.INTEGER)
      log.info(s"creating $numDocs documents ...")
      (1 to numDocs) foreach newDoc
    }.run()

    // select * from className
    val resultAll = execute(
      OrientRDDFetcher.select(className, None, None, OrientRDDFetcherTest.docTransformer)
        .map(_.collect().toList)).get

    resultAll shouldBe (1 to numDocs).toList

    // select * from className where id <= 500
    val resultWithWhereClause = execute(
      OrientRDDFetcher.select(className, None, Some("id <= 500"), OrientRDDFetcherTest.docTransformer)
        .map(_.collect().toList)).get

    resultWithWhereClause shouldBe (1 to numDocs / 2).toList

    // select id from className
    val fieldNames1 = execute(
      OrientRDDFetcher.select(className, Some("id"), None, doc ⇒ doc.fieldNames().toList)
        .map(_.collect())).get.flatten.toSet
    fieldNames1 shouldBe Set("id")

    // select id, value from className
    val fieldNames2 = execute(
      OrientRDDFetcher.select(className, Some("id, value"), None, doc ⇒ doc.fieldNames().toList)
        .map(_.collect())).get.flatten.toSet
    fieldNames2 shouldBe Set("id", "value")
  }

  "Save SparkRDD to OrientDB" in new SimpleTestContext("") {
    val dictators = List(Dictator(1971, "Idi", "Uganda"), Dictator(1980, "Robert", "Zimbabwe"), Dictator(2009, "Jacob", "South Africa"))
    val operation: SparkOperation[Set[Dictator]] = SparkOperation { ctx ⇒
      val rdd = ctx.parallelize(dictators)
      rdd.saveToOrient("Dictators")
      val retrievedRDD = OrientRDDFetcher.select[Dictator]("Dictators", None, Some("year > 1979"), doc ⇒ {
        Dictator(doc.field("year").asInstanceOf[Int], doc.field("name").asInstanceOf[String], doc.field("country").asInstanceOf[String])
      })
      retrievedRDD.run(ctx).collect().toSet
    }

    val savedAndRetrieved = execute(operation)
    savedAndRetrieved.get shouldBe dictators.filter(dic ⇒ dic.year > 1979).toSet
  }
}

object OrientRDDFetcherTest {
  case class Dictator(year: Int, name: String, country: String)
  def docTransformer(doc: ODocument) = {
    doc.field("id").asInstanceOf[Int]
  }
}
