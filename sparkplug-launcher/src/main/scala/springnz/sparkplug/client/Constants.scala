package springnz.sparkplug.client

object Constants {
  val defaultConfigSectionName = "sparkPlugAkkaClient"
  val actorSystemName = "sparkPlugClientSystem"
  val coordinatorActorName = "Coordinator"
  val defaultJarPath = "target/pack/lib"
  val mainJarPattern = "sparkplug-executor*"
  val mainClass = "springnz.sparkplug.executor.ExecutorService"
}
