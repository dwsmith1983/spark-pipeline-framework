import sbt._

/**
 * Custom virtual axis for Spark version targeting.
 * Used by sbt-projectmatrix for cross-building against different Spark versions.
 */
case class SparkAxis(sparkVersion: String, idSuffix: String) extends VirtualAxis.WeakAxis {
  override val directorySuffix: String = s"-$idSuffix"
  override val suffixOrder: Int = 50
}
