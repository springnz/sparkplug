package springnz.sparkplug.examples

import springnz.util.Logging

/**
  * Created by stephen on 9/11/15.
  */
trait WorldTestPipeline extends WorldPipeline with Logging {
  import springnz.sparkplug.testkit.TestExtensions._
  override def dataSource = super.dataSource.sourceFrom("WorldTestData")
}
