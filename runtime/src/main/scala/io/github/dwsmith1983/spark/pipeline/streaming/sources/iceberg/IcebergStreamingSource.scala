package io.github.dwsmith1983.spark.pipeline.streaming.sources.iceberg

import com.typesafe.config.Config
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSource
import io.github.dwsmith1983.spark.pipeline.streaming.config.{IcebergConfig, SchemaEvolutionMode}
import org.apache.spark.sql.DataFrame
import pureconfig._
import pureconfig.generic.auto._

/**
 * Streaming source for Apache Iceberg tables.
 *
 * Reads data incrementally from Iceberg tables using snapshot-based streaming.
 * Each micro-batch processes data from new snapshots since the last batch.
 *
 * == Basic Usage ==
 *
 * {{{
 * val config = IcebergConfig(
 *   tablePath = "spark_catalog.db.events"
 * )
 * val source = new IcebergStreamingSource(config)
 * val df = source.readStream()
 * }}}
 *
 * == Starting from a Timestamp ==
 *
 * Start reading from a specific point in time:
 *
 * {{{
 * val config = IcebergConfig(
 *   tablePath = "spark_catalog.db.events",
 *   streamFromTimestamp = Some(1704067200000L)  // 2024-01-01 00:00:00 UTC
 * )
 * }}}
 *
 * == Filtering Snapshot Types ==
 *
 * Skip certain types of snapshots:
 *
 * {{{
 * val config = IcebergConfig(
 *   tablePath = "spark_catalog.db.events",
 *   skipDeleteSnapshots = true,      // Skip delete-only snapshots
 *   skipOverwriteSnapshots = true    // Skip overwrite snapshots
 * )
 * }}}
 *
 * == Table Path Formats ==
 *
 * The table path can be specified as:
 * - Catalog format: `spark_catalog.database.table` (recommended)
 * - Hadoop path: `/path/to/iceberg/table`
 *
 * @param config Iceberg configuration
 *
 * @see `io.github.dwsmith1983.spark.pipeline.streaming.config.IcebergConfig` for configuration options
 * @see [[https://iceberg.apache.org/docs/latest/spark-structured-streaming/ Iceberg Streaming Guide]]
 */
class IcebergStreamingSource(config: IcebergConfig) extends StreamingSource {

  // Validate source-specific configuration
  config.validateForSource()

  override def name: String =
    s"IcebergStreamingSource[${config.fullTablePath}]"

  /**
   * Creates a streaming DataFrame from the Iceberg table.
   *
   * @return Streaming DataFrame with incremental table contents
   */
  override def readStream(): DataFrame = {
    val reader = spark.readStream.format("iceberg")

    // Apply starting position
    config.streamFromTimestamp.foreach(ts => reader.option("stream-from-timestamp", ts.toString))

    // Apply snapshot filtering options
    if (config.skipDeleteSnapshots) {
      reader.option("streaming-skip-delete-snapshots", "true")
    }
    if (config.skipOverwriteSnapshots) {
      reader.option("streaming-skip-overwrite-snapshots", "true")
    }

    // Apply additional options
    config.options.foreach {
      case (key, value) =>
        reader.option(key, value)
    }

    reader.load(config.fullTablePath)
  }

  // Iceberg doesn't have a built-in event time column
  // Users should specify the appropriate column from their schema
  override def watermarkColumn: Option[String] = None
  override def watermarkDelay: Option[String]  = None
}

/**
 * Factory for creating [[IcebergStreamingSource]] from configuration.
 *
 * == HOCON Configuration Example ==
 *
 * {{{
 * source {
 *   instance-class = "io.github.dwsmith1983.spark.pipeline.streaming.sources.iceberg.IcebergStreamingSource"
 *   instance-config {
 *     table-path = "spark_catalog.db.events"
 *     catalog-name = "spark_catalog"
 *     stream-from-timestamp = 1704067200000
 *     skip-delete-snapshots = true
 *     skip-overwrite-snapshots = false
 *   }
 * }
 * }}}
 */
object IcebergStreamingSource extends ConfigurableInstance {

  private case class HoconConfig(
    tablePath: String,
    catalogName: String = "spark_catalog",
    streamFromTimestamp: Option[Long] = None,
    skipDeleteSnapshots: Boolean = false,
    skipOverwriteSnapshots: Boolean = false,
    schemaEvolutionMode: String = "add-columns",
    options: Map[String, String] = Map.empty)

  override def createFromConfig(conf: Config): IcebergStreamingSource = {
    val hoconConfig = ConfigSource.fromConfig(conf).loadOrThrow[HoconConfig]

    val schemaEvolution = hoconConfig.schemaEvolutionMode.toLowerCase.replace("-", "") match {
      case "none"          => SchemaEvolutionMode.None
      case "addcolumns"    => SchemaEvolutionMode.AddColumns
      case "fullevolution" => SchemaEvolutionMode.FullEvolution
      case other =>
        throw new IllegalArgumentException(s"Unknown schema evolution mode: $other")
    }

    val config = IcebergConfig(
      tablePath = hoconConfig.tablePath,
      catalogName = hoconConfig.catalogName,
      streamFromTimestamp = hoconConfig.streamFromTimestamp,
      skipDeleteSnapshots = hoconConfig.skipDeleteSnapshots,
      skipOverwriteSnapshots = hoconConfig.skipOverwriteSnapshots,
      schemaEvolutionMode = schemaEvolution,
      options = hoconConfig.options
    )

    new IcebergStreamingSource(config)
  }
}
