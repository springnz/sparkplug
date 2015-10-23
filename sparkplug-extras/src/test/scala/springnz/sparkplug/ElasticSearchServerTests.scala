//package springnz.sparkplug
//
//import org.elasticsearch.action.index.IndexRequest
//import org.scalatest.{ ShouldMatchers, WordSpec }
//import springnz.sparkplug.testkit.ElasticsearchServer
//import springnz.util.Logging
//
//class ElasticSearchServerTests extends WordSpec with ShouldMatchers with Logging {
//
//  "elastic search server" should {
//    "start up a new instance" ignore /*("Pretty experimental")*/ {
//      val server = new ElasticsearchServer()
//      server.start()
//      val indexRequest = new IndexRequest("index")
//      val client = server.client
//      client.index(indexRequest)
//      server.stop()
//    }
//  }
//}
