package springnz.sparkplug.elasticsearch

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.NodeBuilder._

import scala.util.Try

// adapted from https://orrsella.com/2014/10/28/embedded-elasticsearch-server-for-scala-integration-tests/
class ElasticsearchServer(clusterName: String) {

  //  private val clusterName = "neon-search"
  private val dataDirPath = Files.createTempDirectory(s"data-$clusterName-")
  private val dataDir = dataDirPath.toFile
  private val settings = Settings.settingsBuilder
    .put("path.home", "/usr/local/elasticsearch-2.0.0/bin")
    .put("path.data", dataDir.toString)
    .put("http.enabled", "true")
    .put("cluster.name", clusterName)
    .build

  private lazy val node = nodeBuilder().local(false).settings(settings).build

  def client: Client = node.client

  def isClosed = node.isClosed

  def start(): Unit = node.start()

  def stop(): Unit = {
    node.close()
    Try { FileUtils.forceDelete(dataDir) }
  }

  def deleteIndex(index: String): Try[Unit] = Try {
    client.admin.indices.prepareDelete(index).execute().actionGet()
  }

  def createAndWaitForIndex(index: String): Unit = {
    client.admin.indices.prepareCreate(index).execute.actionGet()
    client.admin.cluster.prepareHealth(index).setWaitForActiveShards(1).execute.actionGet()
  }
}
//
//object Server {
//  def main(args: Array[String]) {
//    val server = new ElasticsearchServer("neon-search")
//
//    server.start()
////    server.createAndWaitForIndex("ylabs2")
//    while (!server.isClosed) {
//      try {
//        Thread.sleep(60 * 1000)
//      } catch {
//        case _: Throwable ⇒ server.stop()
//      }
//    }
//  }
//}
