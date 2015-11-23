package springnz.sparkplug.orientdb

import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import org.scalatest.{ ShouldMatchers, WordSpec }
import springnz.orientdb.session.ODBSession
import springnz.orientdb.test.ODBMemoryTest
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
        doc.save()
      }
      log.info(s"creating class $className ...")
      createClass(className).createProperty("id", OType.INTEGER)
      log.info(s"creating $numDocs documents ...")
      (1 to numDocs) foreach newDoc
    }.run()

    // select * from className
    val resultAll = execute(
      OrientRDDFetcher.select(className, None, OrientRDDFetcherTest.docTransformer)
        .map(_.collect().toList)).get

    resultAll shouldBe (1 to numDocs).toList

    // select * from className where id <= 500
    val resultWithWhereClause = execute(
      OrientRDDFetcher.select(className, Some("id <= 500"), OrientRDDFetcherTest.docTransformer)
        .map(_.collect().toList)).get

    resultWithWhereClause shouldBe (1 to numDocs / 2).toList
  }

}

object OrientRDDFetcherTest {
  def docTransformer(doc: ODocument) = doc.field("id").asInstanceOf[Int]
}
