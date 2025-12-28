// Scala versions
lazy val scala212 = "2.12.18"
lazy val scala213 = "2.13.12"

// Spark versions
lazy val spark35 = "3.5.7"
lazy val spark40 = "4.0.1"

// Dependency versions
lazy val typesafeConfigVersion = "1.4.3"
lazy val pureConfigVersion = "0.17.4"
lazy val scalatestVersion = "3.2.17"
lazy val log4jVersion = "2.23.1"
lazy val micrometerVersion = "1.12.2"

// Spark version axes (SparkAxis defined in project/SparkAxis.scala)
lazy val Spark3 = SparkAxis(spark35, "spark3")
lazy val Spark4 = SparkAxis(spark40, "spark4")

// Common settings
ThisBuild / organization := "io.github.dwsmith1983"
ThisBuild / version := "0.1.13" // x-release-please-version
ThisBuild / javacOptions ++= Seq("-source", "17", "-target", "17")

// Publishing configuration
ThisBuild / publishMavenStyle := true
ThisBuild / licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / homepage := Some(url("https://dwsmith1983.github.io/spark-pipeline-framework/"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/dwsmith1983/spark-pipeline-framework"),
    "scm:git@github.com:dwsmith1983/spark-pipeline-framework.git"
  )
)
ThisBuild / developers := List(
  Developer("dwsmith1983", "Dustin Smith", "Dustin.William.Smith@gmail.com", url("https://github.com/dwsmith1983"))
)

// Maven Central (Sonatype) publishing via Central Portal
import xerial.sbt.Sonatype.sonatypeCentralHost
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost
ThisBuild / publishTo := sonatypePublishToBundle.value

// Resolve dependency conflicts
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

// =============================================================================
// Code Quality Tools Configuration
// =============================================================================

// Scalafmt - format on compile (optional, can be disabled)
ThisBuild / scalafmtOnCompile := false

// Scalafix - enable SemanticDB for semantic rules (RemoveUnused, etc.)
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

// OWASP Dependency Check - security vulnerability scanning
// Run: sbt dependencyCheck
// Fails build if CVSS score >= 7 (high/critical vulnerabilities)
// NVD API key set via -Danalyzer.nist.nvd.api.key system property in CI
ThisBuild / dependencyCheckFailBuildOnCVSS := 7.0
// Note: Suppression file is loaded via suppressionFile property passed to OWASP engine
// The SuppressionFilesSettings API doesn't work reliably with file() constructor

// Code coverage settings
ThisBuild / coverageMinimumStmtTotal := 75
ThisBuild / coverageFailOnMinimum := true
ThisBuild / coverageHighlighting := true

// =============================================================================
// Helper functions for Spark module settings
// =============================================================================

/**
 * Creates common settings for Spark-dependent modules.
 * @param modulePrefix Base module name (e.g., "spark-pipeline-runtime")
 * @param sparkAxis The Spark version axis (Spark3 or Spark4)
 * @return Settings sequence with moduleName and spark-sql dependency
 */
def sparkModuleSettings(modulePrefix: String, sparkAxis: SparkAxis): Seq[Setting[_]] = Seq(
  moduleName := s"$modulePrefix-${sparkAxis.idSuffix}",
  libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkAxis.sparkVersion % Provided
)

/**
 * Creates assembly settings for executable Spark modules.
 * @param modulePrefix Base module name (e.g., "spark-pipeline-runner")
 * @param sparkAxis The Spark version axis (Spark3 or Spark4)
 * @param mainClassName Fully qualified main class name
 * @return Settings sequence with assembly configuration
 */
def assemblySettings(modulePrefix: String, sparkAxis: SparkAxis, mainClassName: String): Seq[Setting[_]] = Seq(
  assembly / mainClass := Some(mainClassName),
  assembly / assemblyJarName := s"$modulePrefix-${sparkAxis.idSuffix}_${scalaBinaryVersion.value}-${version.value}.jar",
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case "reference.conf"         => MergeStrategy.concat
    case _                        => MergeStrategy.first
  }
)

// Scala compiler options - common to all versions
lazy val commonScalacOptions = Seq(
  "-deprecation",                      // Emit warning for usages of deprecated APIs
  "-encoding", "UTF-8",                // Source file encoding
  "-feature",                          // Emit warning for features requiring explicit import
  "-unchecked",                        // Enable additional warnings for type erasure
  "-Xfatal-warnings",                  // Fail compilation on warnings
  "-Xlint:adapted-args",               // Warn if argument list modified to match receiver
  "-Xlint:constant",                   // Warn when comparing constants
  "-Xlint:delayedinit-select",         // Warn when accessing DelayedInit
  "-Xlint:doc-detached",               // Warn when Scaladoc detached from element
  "-Xlint:inaccessible",               // Warn about inaccessible types in method signatures
  "-Xlint:infer-any",                  // Warn when type inferred to be Any
  "-Xlint:missing-interpolator",       // Warn when string has $ but is not interpolated
  "-Xlint:nullary-unit",               // Warn when nullary method returns Unit
  "-Xlint:option-implicit",            // Warn about implicit conversion from Option
  "-Xlint:package-object-classes",     // Warn about classes in package objects
  "-Xlint:poly-implicit-overload",     // Warn about parameterized overloaded implicit methods
  "-Xlint:private-shadow",             // Warn when private field shadows superclass field
  "-Xlint:stars-align",                // Warn about incorrect use of _ pattern
  "-Xlint:type-parameter-shadow"       // Warn when type parameter shadows outer type
)

// Scala 2.12 specific options
lazy val scala212Options = Seq(
  "-Ypartial-unification",             // Enable partial unification
  "-Ywarn-dead-code",                  // Warn on dead code
  "-Ywarn-unused:imports",             // Warn on unused imports
  "-Ywarn-unused:locals",              // Warn on unused local variables
  "-Ywarn-unused:params",              // Warn on unused parameters
  "-Ywarn-unused:privates",            // Warn on unused private members
  "-Ywarn-value-discard"               // Warn on value discard
)

// Scala 2.13 specific options
lazy val scala213Options = Seq(
  "-Wdead-code",                       // Warn on dead code
  "-Wunused:imports",                  // Warn on unused imports
  "-Wunused:locals",                   // Warn on unused local variables
  "-Wunused:params",                   // Warn on unused parameters
  "-Wunused:privates",                 // Warn on unused private members
  "-Wvalue-discard"                    // Warn on value discard
)

lazy val commonSettings = Seq(
  scalacOptions ++= commonScalacOptions,
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => scala212Options
      case Some((2, 13)) => scala213Options
      case _             => Nil
    }
  },
  // Disable fatal warnings in console and test
  Compile / console / scalacOptions --= Seq("-Xfatal-warnings"),
  Test / scalacOptions --= Seq("-Xfatal-warnings"),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion % Test
  ),
  // Scalastyle configuration - use root directory for config file
  (Compile / scalastyleConfig) := (ThisBuild / baseDirectory).value / "scalastyle-config.xml",
  (Test / scalastyleConfig) := (ThisBuild / baseDirectory).value / "scalastyle-config.xml",
  // Run tests sequentially to avoid SparkSession conflicts
  Test / parallelExecution := false,
  // Fork JVM for tests to isolate SparkSession
  Test / fork := true,
  Test / javaOptions ++= Seq(
    "-Xmx2G",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
  )
)

// ============================================================================
// Core Module - No Spark dependency, pure config parsing and traits
// ============================================================================
lazy val core = (projectMatrix in file("core"))
  .settings(
    name := "spark-pipeline-core",
    commonSettings,
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % typesafeConfigVersion,
      "com.github.pureconfig" %% "pureconfig" % pureConfigVersion,
      "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion % Test,
      "io.micrometer" % "micrometer-core" % micrometerVersion
    )
  )
  .jvmPlatform(scalaVersions = Seq(scala212, scala213))

// ============================================================================
// Runtime Module - Spark-dependent, provides SparkSessionWrapper and DataFlow
// ============================================================================
lazy val runtimeModulePrefix = "spark-pipeline-runtime"

lazy val runtime = (projectMatrix in file("runtime"))
  .dependsOn(core)
  .settings(
    name := "spark-pipeline-runtime",
    commonSettings
  )
  .customRow(
    scalaVersions = Seq(scala212),
    axisValues = Seq(Spark3, VirtualAxis.jvm),
    settings = sparkModuleSettings(runtimeModulePrefix, Spark3)
  )
  .customRow(
    scalaVersions = Seq(scala213),
    axisValues = Seq(Spark3, VirtualAxis.jvm),
    settings = sparkModuleSettings(runtimeModulePrefix, Spark3)
  )
  .customRow(
    scalaVersions = Seq(scala213),
    axisValues = Seq(Spark4, VirtualAxis.jvm),
    settings = sparkModuleSettings(runtimeModulePrefix, Spark4)
  )

// ============================================================================
// Runner Module - Entry point with SimplePipelineRunner
// ============================================================================
lazy val runnerModulePrefix = "spark-pipeline-runner"
lazy val runnerMainClass    = "io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner"

lazy val runner = (projectMatrix in file("runner"))
  .dependsOn(runtime)
  .settings(
    name := "spark-pipeline-runner",
    commonSettings
  )
  .customRow(
    scalaVersions = Seq(scala212),
    axisValues = Seq(Spark3, VirtualAxis.jvm),
    settings = sparkModuleSettings(runnerModulePrefix, Spark3) ++
      assemblySettings(runnerModulePrefix, Spark3, runnerMainClass)
  )
  .customRow(
    scalaVersions = Seq(scala213),
    axisValues = Seq(Spark3, VirtualAxis.jvm),
    settings = sparkModuleSettings(runnerModulePrefix, Spark3) ++
      assemblySettings(runnerModulePrefix, Spark3, runnerMainClass)
  )
  .customRow(
    scalaVersions = Seq(scala213),
    axisValues = Seq(Spark4, VirtualAxis.jvm),
    settings = sparkModuleSettings(runnerModulePrefix, Spark4) ++
      assemblySettings(runnerModulePrefix, Spark4, runnerMainClass)
  )

// ============================================================================
// Example Module - Shows how to use the framework
// ============================================================================
lazy val exampleModulePrefix = "spark-pipeline-example"

lazy val example = (projectMatrix in file("example"))
  .dependsOn(runtime, runner)
  .settings(
    name := "spark-pipeline-example",
    commonSettings,
    publish / skip := true,
    // Exclude demo files from coverage - runnable demos with main() don't need coverage
    coverageExcludedFiles := ".*Demo.*Pipeline.*;.*Demo.*Hooks.*;.*ValidationDemo.*;.*FailingComponent.*"
  )
  .customRow(
    scalaVersions = Seq(scala212),
    axisValues = Seq(Spark3, VirtualAxis.jvm),
    settings = sparkModuleSettings(exampleModulePrefix, Spark3)
  )
  .customRow(
    scalaVersions = Seq(scala213),
    axisValues = Seq(Spark3, VirtualAxis.jvm),
    settings = sparkModuleSettings(exampleModulePrefix, Spark3)
  )
  .customRow(
    scalaVersions = Seq(scala213),
    axisValues = Seq(Spark4, VirtualAxis.jvm),
    settings = sparkModuleSettings(exampleModulePrefix, Spark4)
  )

// Root project
lazy val root = (project in file("."))
  .aggregate(
    core.projectRefs ++
    runtime.projectRefs ++
    runner.projectRefs ++
    example.projectRefs: _*
  )
  .settings(
    name := "spark-pipeline-framework",
    publish / skip := true
  )
