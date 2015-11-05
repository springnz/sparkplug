package ylabs.elasticsearch

import java.nio.file.Files

import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.NodeBuilder._

import scala.util.Try

// kindly provided by https://orrsella.com/2014/10/28/embedded-elasticsearch-server-for-scala-integration-tests/
class ElasticsearchServer {

  private val clusterName = "elasticsearch"
  private val dataDirPath = Files.createTempDirectory("elasticsearch_data_")
  private val dataDir = dataDirPath.toFile
  private val settings = Settings.settingsBuilder
    .put("path.home", "/usr/local/elasticsearch-2.0.0/core")
    .put("path.data", dataDir.toString)
    .put("http.enabled", "true")
    .put("cluster.name", clusterName)
    .put("security.manager.enabled", false)
    .build

  private lazy val node = nodeBuilder().local(false).settings(settings).build

  def client: Client = node.client

  def start(): Unit = node.start()

  def stop(): Unit = {
    node.close()

    Try { Files.deleteIfExists(dataDirPath) }
  }

  def createAndWaitForIndex(index: String): Unit = {
    client.admin.indices.prepareCreate(index).execute.actionGet()
    client.admin.cluster.prepareHealth(index).setWaitForActiveShards(1).execute.actionGet()
  }
}

object Server {
  def main (args: Array[String]){
    val server = new ElasticsearchServer

    server.start()
    server.createAndWaitForIndex("ylabs")
  }
}
