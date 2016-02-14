package springnz.sparkplug.examples

import springnz.sparkplug.util.Logging

/**
  * Created by stephen on 9/11/15.
  */
trait WorldTestPipeline extends WorldPipeline with Logging {
  import springnz.sparkplug.testkit._

  override def dataSource = super.dataSource.sourceFrom("WorldTestData")
}
