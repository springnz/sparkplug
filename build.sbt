// refer to project/Common.scala for shared settings and definitions
// refer to project/Dependencies.scala for dependency definitions
import Common._
import Dependencies._
import sbt.Keys._
import xerial.sbt.Pack._

name := "sparkplug"
organization := organisationString
scalaVersion := scalaVersionString

releaseVersionBump := sbtrelease.Version.Bump.Bugfix

// run the tests in series
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

def dep(project: Project) = project % "test->test;compile->compile"

lazy val sparkPlugCore: Project = CreateProject("sparkplug-core", sparkCoreLibDependencies)

lazy val sparkPlugExtras = CreateProject("sparkplug-extras", sparkExtraLibDependencies)
  .dependsOn(dep(sparkPlugCore))
  .settings(fork := true) // required for OrientDB tests

lazy val sparkPlugExamples = CreateProject("sparkplug-examples", sparkExampleLibDependencies)
  .dependsOn(dep(sparkPlugCore), dep(sparkPlugExtras))

lazy val sparkExecutor = CreateProject("sparkplug-executor", sparkExecutorLibDependencies)
  .dependsOn(dep(sparkPlugCore), sparkPlugExamples % "test->compile")

lazy val sparkLauncher = CreateProject("sparkplug-launcher", sparkLauncherLibDependencies)
  .dependsOn(dep(sparkPlugCore), dep(sparkExecutor))

lazy val main = project.in(file("."))
  .aggregate(sparkPlugCore, sparkPlugExtras, sparkPlugExamples, sparkExecutor, sparkLauncher)
  .settings(Defaults.coreDefaultSettings ++ Seq(
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
    publishArtifact := false
  ))
  .settings(packAutoSettings)
  .settings(packGenerateWindowsBatFile := false)
  .settings(parallelExecution in Test := false)


