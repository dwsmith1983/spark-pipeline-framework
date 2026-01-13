package io.github.dwsmith1983.spark.pipeline.streaming.sources.delta

import com.typesafe.config.Config
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSource
import io.github.dwsmith1983.spark.pipeline.streaming.config.{DeltaLakeConfig, SchemaEvolutionMode}
import org.apache.spark.sql.DataFrame
import pureconfig._
import pureconfig.generic.auto._

/**
 * Streaming source for Delta Lake tables.
 *
 * Reads data from Delta Lake tables as a continuous stream. Supports
 * Change Data Feed (CDC) for capturing row-level changes.
 *
 * == Basic Usage ==
 *
 * {{{
 * val config = DeltaLakeConfig(
 *   path = "/data/events"
 * )
 * val source = new DeltaStreamingSource(config)
 * val df = source.readStream()
 * }}}
 *
 * == Change Data Feed (CDC) ==
 *
 * Enable Change Data Feed to capture INSERT, UPDATE, and DELETE operations:
 *
 * {{{
 * val config = DeltaLakeConfig(
 *   path = "/data/events",
 *   readChangeDataFeed = true,
 *   startingVersion = Some(10L)
 * )
 * val source = new DeltaStreamingSource(config)
 * val cdcDF = source.readStream()
 * // cdcDF includes: _change_type, _commit_version, _commit_timestamp
 * }}}
 *
 * == Output Schema (CDC enabled) ==
 *
 * When `readChangeDataFeed = true`, additional columns are included:
 * {{{
 * root
 *  |-- ...original columns...
 *  |-- _change_type: string (insert, update_preimage, update_postimage, delete)
 *  |-- _commit_version: long
 *  |-- _commit_timestamp: timestamp
 * }}}
 *
 * == Prerequisites ==
 *
 * For CDC, the Delta table must have Change Data Feed enabled:
 * {{{
 * ALTER TABLE delta.`/data/events` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
 * }}}
 *
 * @param config Delta Lake configuration
 *
 * @see [[io.github.dwsmith1983.spark.pipeline.streaming.config.DeltaLakeConfig]] for configuration options
 * @see [[https://docs.delta.io/latest/delta-streaming.html Delta Streaming Guide]]
 */
class DeltaStreamingSource(config: DeltaLakeConfig) extends StreamingSource {

  // Validate source-specific configuration
  config.validateForSource()

  override def name: String =
    if (config.readChangeDataFeed) s"DeltaStreamingSource[CDC:${config.path}]"
    else s"DeltaStreamingSource[${config.path}]"

  /**
   * Creates a streaming DataFrame from the Delta table.
   *
   * @return Streaming DataFrame with table contents (or CDC events)
   */
  override def readStream(): DataFrame = {
    val reader = spark.readStream.format("delta")

    // Apply CDC options
    if (config.readChangeDataFeed) {
      reader.option("readChangeFeed", "true")
      config.startingVersion.foreach(v => reader.option("startingVersion", v))
      config.startingTimestamp.foreach(ts => reader.option("startingTimestamp", ts))
    }

    // Apply streaming options
    reader
      .option("ignoreChanges", config.ignoreChanges)
      .option("ignoreDeletes", config.ignoreDeletes)
      .option("maxFilesPerTrigger", config.maxFilesPerTrigger)

    // Apply additional options
    config.options.foreach {
      case (key, value) =>
        reader.option(key, value)
    }

    reader.load(config.path)
  }

  // Delta doesn't have a built-in timestamp column for watermarking
  // Users should specify the appropriate column from their schema
  override def watermarkColumn: Option[String] = None
  override def watermarkDelay: Option[String]  = None
}

/**
 * Factory for creating [[DeltaStreamingSource]] from configuration.
 *
 * == HOCON Configuration Example ==
 *
 * {{{
 * source {
 *   instance-class = "io.github.dwsmith1983.spark.pipeline.streaming.sources.delta.DeltaStreamingSource"
 *   instance-config {
 *     path = "/data/events"
 *     read-change-data-feed = true
 *     starting-version = 10
 *     max-files-per-trigger = 1000
 *     ignore-changes = false
 *     ignore-deletes = false
 *   }
 * }
 * }}}
 */
object DeltaStreamingSource extends ConfigurableInstance {

  private case class HoconConfig(
    path: String,
    readChangeDataFeed: Boolean = false,
    startingVersion: Option[Long] = None,
    startingTimestamp: Option[String] = None,
    ignoreChanges: Boolean = false,
    ignoreDeletes: Boolean = false,
    maxFilesPerTrigger: Int = 1000,
    schemaEvolutionMode: String = "add-columns",
    options: Map[String, String] = Map.empty)

  override def createFromConfig(conf: Config): DeltaStreamingSource = {
    val hoconConfig = ConfigSource.fromConfig(conf).loadOrThrow[HoconConfig]

    val schemaEvolution = hoconConfig.schemaEvolutionMode.toLowerCase.replace("-", "") match {
      case "none"          => SchemaEvolutionMode.None
      case "addcolumns"    => SchemaEvolutionMode.AddColumns
      case "fullevolution" => SchemaEvolutionMode.FullEvolution
      case other =>
        throw new IllegalArgumentException(s"Unknown schema evolution mode: $other")
    }

    val config = DeltaLakeConfig(
      path = hoconConfig.path,
      readChangeDataFeed = hoconConfig.readChangeDataFeed,
      startingVersion = hoconConfig.startingVersion,
      startingTimestamp = hoconConfig.startingTimestamp,
      ignoreChanges = hoconConfig.ignoreChanges,
      ignoreDeletes = hoconConfig.ignoreDeletes,
      maxFilesPerTrigger = hoconConfig.maxFilesPerTrigger,
      schemaEvolutionMode = schemaEvolution,
      options = hoconConfig.options
    )

    new DeltaStreamingSource(config)
  }
}
