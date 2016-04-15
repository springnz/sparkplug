package springnz.sparkplug.client

object Constants {
  val defaultAkkaConfigSection = "akkaClient"
  val defaultSparkConfigSection = "spark.conf"
  val actorSystemName = "sparkplugClientSystem"
  val coordinatorActorName = "Coordinator"
  val defaultJarPath = "target/pack/lib"
  val mainJarPattern = "sparkplug-executor*"
  val mainClass = "springnz.sparkplug.executor.ExecutorService"
}
