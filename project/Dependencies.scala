import sbt._

object Dependencies {

  // Version Numbers
  val sparkVersion = "1.4.1"
  val akkaVersion = "2.3.12"
  val cassandraConnectorVersionMap = Map(
    "1.5.1" -> "1.5.0-M2",
    "1.4.1" -> "1.4.0")

  // Spark
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % Provided
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
  val sparkLauncher = "org.apache.spark" %% "spark-launcher" % sparkVersion % Provided
  val elasticSearch = "org.elasticsearch" % "elasticsearch" % "1.7.2" % Provided

  // Spark Data
  val sparkCassandraConnector = "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersionMap(sparkVersion)
  val sparkESConnector = "org.elasticsearch" %% "elasticsearch-spark" % "2.2.0-m1"

  // MacWire
  val macWireMacros = "com.softwaremill.macwire" %% "macros" % "1.0.5"
  val macWireRuntime = "com.softwaremill.macwire" %% "runtime" % "1.0.5"

  // Akka Dependencies
   val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
  val akkaActors = "com.typesafe.akka" %% "akka-actor" % akkaVersion
  val akkaRemote = "com.typesafe.akka" %% "akka-remote" % akkaVersion

  // Other libraries
  val scalaz = "org.scalaz" %% "scalaz-core" % "7.1.3"
  val jodaTime = "joda-time" % "joda-time" % "2.8.2"
  val mySqlDriver = "mysql" % "mysql-connector-java" % "5.1.36"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

  // logging
  val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.10"
  val logBackClassic = "ch.qos.logback" % "logback-classic" % "1.1.3"
  val logBackCore = "ch.qos.logback" % "logback-core" % "1.1.3"
  val logBackDependencies = Seq(logBackClassic, logBackCore)

  // Shared compile
  val playJson = "com.typesafe.play" %% "play-json" % "2.4.2" exclude ("org.slf4j", "slf4j-log4j12")
  val betterFiles = "com.github.pathikrit" %% "better-files" % "2.4.1"

  // SpringNZ projects
  val neonContracts = "ylabs" %% "neon-contracts" % "1.0.0"
  val utilLib = "springnz" %% "util-lib" % "2.3.0"

  // Share test
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.4" % Test
  val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.12.4" % Test

  // Dependency overrides
  // This override is needed because Spark uses a later version of Jackson that breaks play-json
  val jacksonOverride = "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"

  val sparkCoreDependencies = Seq(scalaz, sparkCore, sparkSql)
  val sparkDataDependencies = Seq(sparkCassandraConnector, sparkESConnector, mySqlDriver)
  val macWireDependencies = Seq(macWireMacros, macWireRuntime)

  val akkaDependencies = Seq(akkaActors, akkaTestkit, akkaRemote)

  val sharedCompileDependencies = Seq(slf4jApi, playJson, betterFiles, utilLib)
  val sharedTestDependencies = Seq(scalaTest, scalaCheck)
  val sharedDependencies = sharedCompileDependencies ++ sharedTestDependencies

  val sparkCoreLibDependencies = Seq(scalaLogging) ++ sparkCoreDependencies

  val sparkExtraLibDependencies =sparkCoreLibDependencies ++
    sparkDataDependencies ++
    sharedDependencies

  val sparkExampleLibDependencies = sparkExtraLibDependencies ++
    macWireDependencies ++
    logBackDependencies

  val sparkExecutorLibDependencies = sparkCoreLibDependencies ++
    akkaDependencies ++
    logBackDependencies ++
    sharedDependencies

  val sparkLauncherLibDependencies = Seq(sparkLauncher) ++ akkaDependencies ++ sharedDependencies

  val dependencyOverridesSet = Set(jacksonOverride)

}
