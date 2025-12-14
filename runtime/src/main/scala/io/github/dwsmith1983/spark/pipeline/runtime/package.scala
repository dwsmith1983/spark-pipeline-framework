package io.github.dwsmith1983.spark.pipeline

/**
 * Runtime package for spark-pipeline-framework.
 *
 * This package provides Spark-specific runtime components:
 *
 * - [[runtime.SparkSessionWrapper]] - Trait providing managed SparkSession access
 * - [[runtime.DataFlow]] - Base trait for user pipeline components
 *
 * User pipeline components should extend [[runtime.DataFlow]] which provides:
 * - `spark: SparkSession` - Pre-configured Spark session
 * - `logInfo`, `logDebug`, `logWarning`, `logError` - Spark's native logging (Log4j2)
 * - `run(): Unit` - Abstract method to implement business logic
 *
 * Example user build.sbt:
 * {{{
 * libraryDependencies += "io.github.dwsmith1983" %% "spark-pipeline-runtime-spark3" % "0.1.0"
 * }}}
 */
package object runtime
