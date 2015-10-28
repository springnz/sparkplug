package springnz.sparkplug.client

object Constants {
  val defaultConfigSectionName = "sparkPlugAkkaClient"
  val actorSystemName = "sparkPlugClientSystem"
  val clientActorName = "sparkPlugClient"
  val jarPath = "target/pack/lib"
  val mainJar = s"sparkplug-executor_2.11-0.2.6-SNAPSHOT.jar"
  val mainClass = "springnz.sparkplug.executor.ExecutorService"
}
