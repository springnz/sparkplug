import Dependencies._
import com.typesafe.sbt.SbtScalariform._
import com.typesafe.sbt.packager.docker.DockerPlugin
import com.typesafe.sbt.packager.universal.UniversalPlugin
import sbt.Keys._
import sbt._

import scalariform.formatter.preferences._

object Common {
  val repo = "https://nexus.prod.corp/content"
  val organisationString = "springnz"
  val scalaVersionString = "2.11.7"

  lazy val allResolvers = Seq(
    Resolver.mavenLocal,
    "ylabs" at s"$repo/groups/public",
    "Typesafe repository snapshots" at "http://repo.typesafe.com/typesafe/snapshots/",
    "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/",
    "Java.net Maven2 Repository" at "http://download.java.net/maven/2/",
    "sonatype-oss" at "http://oss.sonatype.org/content/repositories/snapshots",
    "sonatype-oss-public" at "https://oss.sonatype.org/content/groups/public/",
    Resolver.bintrayRepo("pathikrit", "maven"), // for betterfiles
    "gphat" at "https://raw.github.com/gphat/mvn-repo/master/releases/" // for wabisabi
    )

  lazy val commonSettings = Seq(
    organization := organisationString,
    scalaVersion := scalaVersionString,
    scalacOptions := Seq("-Xlint", "-deprecation", "-feature"),
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
    publishTo := {
      if (isSnapshot.value)
        Some("snapshots" at s"$repo/repositories/snapshots")
      else
        Some("releases" at s"$repo/repositories/releases")
    },
    exportJars := true,
    resolvers ++= allResolvers,
    parallelExecution in Test := false,
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)),
    runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run)),
    run in Test <<= Defaults.runTask(fullClasspath in Test, mainClass in (Test, run), runner in (Test, run)),
    runMain in Test <<= Defaults.runMainTask(fullClasspath in Test, runner in (Test, run)),
    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
    testGrouping in Test := {
      val original: Seq[Tests.Group] = (testGrouping in Test).value

      original.map { group ⇒
        val forkOptions = ForkOptions(
          bootJars = Nil,
          javaHome = javaHome.value,
          connectInput = connectInput.value,
          outputStrategy = outputStrategy.value,
          runJVMOptions = javaOptions.value,
          workingDirectory = Some(new File(System.getProperty("user.dir"))),
          envVars = envVars.value)

        group.copy(runPolicy = Tests.SubProcess(forkOptions))
      }
    })

  lazy val scalariformPreferences = ScalariformKeys.preferences := ScalariformKeys.preferences.value
    .setPreference(AlignParameters, false)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 40)
    .setPreference(CompactControlReadability, false)
    .setPreference(CompactStringConcatenation, false)
    .setPreference(DoubleIndentClassDeclaration, true)
    .setPreference(FormatXml, true)
    .setPreference(IndentLocalDefs, false)
    .setPreference(IndentPackageBlocks, true)
    .setPreference(IndentSpaces, 2)
    .setPreference(IndentWithTabs, false)
    .setPreference(MultilineScaladocCommentsStartOnFirstLine, false)
    .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, true)
    .setPreference(PreserveDanglingCloseParenthesis, false)
    .setPreference(PreserveSpaceBeforeArguments, false)
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(SpaceBeforeColon, false)
    .setPreference(SpaceInsideBrackets, false)

  def CreateProject(projName: String, libraryDeps: Seq[ModuleID]) =
    Project(projName, file(projName))
      .settings(commonSettings: _*)
      .settings(libraryDependencies ++= libraryDeps ++ sharedDependencies)
      .settings(dependencyOverrides ++= dependencyOverridesSet)
      // TODO: remove comment when sure we don't need
      // .settings(libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) })
      .settings(name := projName)
      .enablePlugins(UniversalPlugin)
      .enablePlugins(DockerPlugin)
      .settings(scalariformSettings)
      .settings(scalariformPreferences)
      .settings(scalaSource in Compile <<= baseDirectory(_ / s"src/main/scala"))
      .settings(scalaSource in Test <<= baseDirectory(_ / s"src/test/scala"))
      .settings(resourceDirectory in Compile <<= baseDirectory(_ / s"src/main/resources"))
      .settings(resourceDirectory in Test <<= baseDirectory(_ / s"src/test/resources"))

}
