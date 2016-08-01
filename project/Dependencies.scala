import sbt._

object Dependencies {

  // Version Numbers
  val sparkVersion = "1.6.2"
  val akkaVersion = "2.3.12"

  // Spark
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % Provided
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
  val sparkLauncher = "org.apache.spark" %% "spark-launcher" % sparkVersion % Provided

  // Akka Dependencies
  val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test
  val akkaActors = "com.typesafe.akka" %% "akka-actor" % akkaVersion
  val akkaRemote = "com.typesafe.akka" %% "akka-remote" % akkaVersion
  val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % akkaVersion

  // Other libraries
  val scalaz = "org.scalaz" %% "scalaz-core" % "7.1.3"

  // logging
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
  val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.10"
  val logBackCore = "ch.qos.logback" % "logback-core" % "1.1.3"
  val logBackClassic = "ch.qos.logback" % "logback-classic" % "1.1.3"
  val logBackDependencies = Seq(logBackClassic, logBackCore)
  val logBackCoreTest = "ch.qos.logback" % "logback-core" % "1.1.3" % Test
  val logBackClassicTest = "ch.qos.logback" % "logback-classic" % "1.1.3" % Test
  val logBackTestDependencies = Seq(logBackCoreTest, logBackClassicTest)

  // Shared compile
  val json4sJackson = "org.json4s" %% "json4s-jackson" % "3.2.10"
  val betterFiles = "com.github.pathikrit" %% "better-files" % "2.14.0"

  // Share test
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.4" % Test
  val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.12.4" % Test

  val sparkCoreDependencies = Seq(scalaz, sparkCore, sparkSql)

  val akkaDependencies = Seq(akkaActors, akkaTestkit, akkaRemote, akkaSlf4j)

  val sharedCompileDependencies = Seq(slf4jApi, json4sJackson, betterFiles)
  val sharedTestDependencies = Seq(scalaTest, scalaCheck)
  val sharedDependencies = sharedCompileDependencies ++ sharedTestDependencies

  val sparkCoreLibDependencies = Seq(scalaLogging) ++ sparkCoreDependencies

  val sparkExecutorLibDependencies = sparkCoreLibDependencies ++
    akkaDependencies ++
    logBackDependencies ++
    sharedDependencies

  val sparkLauncherLibDependencies = Seq(sparkLauncher, scalaLogging) ++
    akkaDependencies ++
    logBackDependencies ++
    sharedDependencies

  // Dependency overrides
  // This override is needed because Spark uses a later version of Jackson that breaks play-json
//  val jacksonOverride = "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
  val guavaOverride = "com.google.guava" % "guava" % "18.0"

  val sparkOverrides = Set(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-launcher" % sparkVersion
  )

  val dependencyOverridesSet = Set(/*jacksonOverride,*/ guavaOverride) ++ sparkOverrides
}

