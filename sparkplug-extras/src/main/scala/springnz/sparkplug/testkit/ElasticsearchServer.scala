//package springnz.sparkplug.testkit
//
//import java.nio.file.Files
//
//import org.apache.commons.io.FileUtils
//import org.elasticsearch.client.Client
//import org.elasticsearch.common.settings.ImmutableSettings
//import org.elasticsearch.node.NodeBuilder._
//
//import scala.util.Try
//
//// kindly provided by https://orrsella.com/2014/10/28/embedded-elasticsearch-server-for-scala-integration-tests/
//class ElasticsearchServer {
//
//  private val clusterName = "elasticsearch"
//  private val dataDir = Files.createTempDirectory("elasticsearch_data_").toFile
//  private val settings = ImmutableSettings.settingsBuilder
//    .put("path.data", dataDir.toString)
//    .put("http.enabled", "true")
//    .put("cluster.name", clusterName)
//    .build
//
//  private lazy val node = nodeBuilder().local(false).settings(settings).build
//
//  def client: Client = node.client
//
//  def start(): Unit = node.start()
//
//  def stop(): Unit = {
//    node.close()
//
//    Try { FileUtils.forceDelete(dataDir) }
//  }
//
//  def createAndWaitForIndex(index: String): Unit = {
//    client.admin.indices.prepareCreate(index).execute.actionGet()
//    client.admin.cluster.prepareHealth(index).setWaitForActiveShards(1).execute.actionGet()
//  }
//}
//
