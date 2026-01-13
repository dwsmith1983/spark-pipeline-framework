package io.github.dwsmith1983.spark.pipeline.streaming.config

/**
 * Configuration for Apache Iceberg streaming sources and sinks.
 *
 * This configuration is shared between Iceberg streaming sources
 * and Iceberg streaming sinks. It supports both incremental reads (streaming from
 * snapshots) and various write modes (append, upsert, overwrite).
 *
 * == Source Configuration ==
 *
 * When using Iceberg as a streaming source, you can read incrementally from
 * table snapshots. Optionally specify a starting timestamp to begin from.
 *
 * {{{
 * val sourceConfig = IcebergConfig(
 *   tablePath = "spark_catalog.db.events",
 *   streamFromTimestamp = Some(1704067200000L)  // 2024-01-01 00:00:00 UTC
 * )
 * }}}
 *
 * == Sink Configuration ==
 *
 * When writing to Iceberg, choose the appropriate write mode:
 *
 * {{{
 * // Append mode (default, most efficient)
 * val appendConfig = IcebergConfig(
 *   tablePath = "spark_catalog.db.events",
 *   writeMode = IcebergWriteMode.Append
 * )
 *
 * // Upsert mode (merge based on key columns)
 * val upsertConfig = IcebergConfig(
 *   tablePath = "spark_catalog.db.events",
 *   writeMode = IcebergWriteMode.Upsert,
 *   upsertKeys = List("event_id", "event_date")
 * )
 *
 * // Dynamic partition overwrite
 * val overwriteConfig = IcebergConfig(
 *   tablePath = "spark_catalog.db.events",
 *   writeMode = IcebergWriteMode.OverwriteDynamic
 * )
 * }}}
 *
 * == Table Path Format ==
 *
 * The table path can be specified in several formats:
 *  - Catalog format: `catalog.database.table` (recommended)
 *  - Path format: `/path/to/iceberg/table`
 *  - Hadoop catalog: `hadoop_catalog.db.table`
 *
 * @param tablePath              Path to the Iceberg table (catalog.db.table or file path)
 * @param catalogName            Name of the Iceberg catalog (default: spark_catalog)
 * @param streamFromTimestamp    Start streaming from snapshots after this timestamp (epoch ms)
 * @param skipDeleteSnapshots    Skip snapshots that only contain deletes (source only)
 * @param skipOverwriteSnapshots Skip snapshots from overwrite operations (source only)
 * @param writeMode              Write mode: Append, Upsert, or OverwriteDynamic (sink only)
 * @param upsertKeys             Key columns for upsert operations (required when writeMode = Upsert)
 * @param fanoutEnabled          Enable fanout writes for better parallelism (sink only)
 * @param partitionBy            Columns to partition by when creating new tables (sink only)
 * @param schemaEvolutionMode    How to handle schema evolution
 * @param options                Additional Iceberg options passed to reader/writer
 *
 * @see [[https://iceberg.apache.org/docs/latest/spark-structured-streaming/ Iceberg Streaming Guide]]
 * @see [[IcebergWriteMode]] for write mode options
 * @see [[SchemaEvolutionMode]] for schema evolution options
 */
final case class IcebergConfig(
  tablePath: String,
  catalogName: String = "spark_catalog",
  // Source-specific options
  streamFromTimestamp: Option[Long] = None,
  skipDeleteSnapshots: Boolean = false,
  skipOverwriteSnapshots: Boolean = false,
  // Sink-specific options
  writeMode: IcebergWriteMode = IcebergWriteMode.Append,
  upsertKeys: List[String] = List.empty,
  fanoutEnabled: Boolean = false,
  partitionBy: List[String] = List.empty,
  // Shared options
  schemaEvolutionMode: SchemaEvolutionMode = SchemaEvolutionMode.AddColumns,
  options: Map[String, String] = Map.empty) {

  /**
   * Returns the fully qualified table identifier.
   *
   * If the tablePath already contains catalog information (has dots),
   * it's returned as-is. Otherwise, the catalogName is prepended.
   *
   * @return The fully qualified table path
   */
  def fullTablePath: String =
    if (tablePath.contains(".")) tablePath
    else s"$catalogName.$tablePath"

  /**
   * Validates the configuration for use as a streaming source.
   *
   * Currently performs no additional validation beyond field types.
   */
  def validateForSource(): Unit = {
    // No specific validation needed for source
  }

  /**
   * Validates the configuration for use as a streaming sink.
   *
   * Throws IllegalArgumentException if upsert mode is used without upsertKeys.
   */
  def validateForSink(): Unit =
    writeMode match {
      case IcebergWriteMode.Upsert =>
        require(
          upsertKeys.nonEmpty,
          "upsertKeys must be non-empty when writeMode is Upsert"
        )
      case _ => // No additional validation needed
    }
}

/** Companion object for [[IcebergConfig]]. */
object IcebergConfig {

  /**
   * Creates a minimal source configuration for reading an Iceberg table.
   *
   * @param tablePath Path to the Iceberg table
   * @return An IcebergConfig configured for streaming reads
   */
  def forSource(tablePath: String): IcebergConfig =
    IcebergConfig(tablePath = tablePath)

  /**
   * Creates a source configuration starting from a specific timestamp.
   *
   * @param tablePath           Path to the Iceberg table
   * @param streamFromTimestamp Epoch milliseconds to start streaming from
   * @return An IcebergConfig configured for timestamp-based streaming
   */
  def forSourceFromTimestamp(tablePath: String, streamFromTimestamp: Long): IcebergConfig =
    IcebergConfig(
      tablePath = tablePath,
      streamFromTimestamp = Some(streamFromTimestamp)
    )

  /**
   * Creates a sink configuration for appending to an Iceberg table.
   *
   * @param tablePath   Path to the Iceberg table
   * @param partitionBy Optional partitioning columns
   * @return An IcebergConfig configured for append writes
   */
  def forAppend(tablePath: String, partitionBy: List[String] = List.empty): IcebergConfig =
    IcebergConfig(
      tablePath = tablePath,
      writeMode = IcebergWriteMode.Append,
      partitionBy = partitionBy
    )

  /**
   * Creates a sink configuration for upsert operations.
   *
   * @param tablePath  Path to the Iceberg table
   * @param upsertKeys Key columns for matching rows
   * @return An IcebergConfig configured for upsert writes
   */
  def forUpsert(tablePath: String, upsertKeys: List[String]): IcebergConfig =
    IcebergConfig(
      tablePath = tablePath,
      writeMode = IcebergWriteMode.Upsert,
      upsertKeys = upsertKeys
    )
}
