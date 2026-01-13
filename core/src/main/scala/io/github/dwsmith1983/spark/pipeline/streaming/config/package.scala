package io.github.dwsmith1983.spark.pipeline.streaming

/**
 * Configuration classes for streaming sinks.
 *
 * This package contains Spark-independent configuration models for
 * streaming sinks. These configs can be loaded from HOCON files via
 * PureConfig and passed to the corresponding sink implementations
 * in the runtime module.
 *
 * == Available Configurations ==
 *
 * - [[config.KafkaSinkConfig]]: Kafka producer with exactly-once semantics
 * - [[config.CloudStorageConfig]]: S3/GCS/ADLS file output
 * - [[config.ForeachSinkConfig]]: Custom processing via foreachBatch/foreach
 *
 * == Shared Configurations (with streaming sources) ==
 *
 * Delta Lake and Iceberg configurations are shared between sources
 * and sinks. See:
 * - `DeltaLakeConfig`: Shared Delta Lake configuration
 * - `IcebergConfig`: Shared Iceberg configuration
 *
 * These shared configs are created by the streaming sources module
 * and used by both source and sink implementations.
 */
package object config {

  /**
   * Output mode for streaming sinks.
   *
   * Maps to Spark's OutputMode but as a string for config files.
   */
  object OutputModeConfig {
    val Append = "append"
    val Complete = "complete"
    val Update = "update"

    def validate(mode: String): Unit = {
      val valid = Set(Append, Complete, Update)
      if (!valid.contains(mode.toLowerCase)) {
        throw new IllegalArgumentException(
          s"Invalid output mode: '$mode'. Must be one of: ${valid.mkString(", ")}"
        )
      }
    }
  }
}
