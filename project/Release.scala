import sbt.Keys._
import sbt.{Project, _}
import sbtrelease.ReleasePlugin.autoImport.ReleaseStep
import sbtrelease.ReleaseStateTransformations._
import xerial.sbt.Pack._

object Release {

  lazy val runPack: ReleaseStep = ReleaseStep(
    action = { st: State =>
      val extracted = Project.extract(st)
      val ref = extracted.get(thisProjectRef)
      extracted.runAggregated(pack in Global in ref, st)
    }
  )

  def customReleaseProcess = Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runPack,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      publishArtifacts,
      setNextVersion,
      commitNextVersion,
      pushChanges
    )
}
