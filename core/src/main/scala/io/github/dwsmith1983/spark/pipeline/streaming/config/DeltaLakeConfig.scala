package io.github.dwsmith1983.spark.pipeline.streaming.config

/**
 * Configuration for Delta Lake streaming sources and sinks.
 *
 * This configuration is shared between Delta streaming sources
 * and Delta streaming sinks. It supports both reading (with Change Data Feed)
 * and writing (with merge, append, overwrite modes).
 *
 * == Source Configuration ==
 *
 * When using Delta Lake as a streaming source, you can read the table as a
 * continuous stream of changes. Enable `readChangeDataFeed` to get CDC events.
 *
 * {{{
 * val sourceConfig = DeltaLakeConfig(
 *   path = "/data/events",
 *   readChangeDataFeed = true,
 *   startingVersion = Some(10L)
 * )
 * }}}
 *
 * == Sink Configuration ==
 *
 * When writing to Delta Lake, choose the appropriate write mode:
 *
 * {{{
 * // Append mode (default)
 * val appendConfig = DeltaLakeConfig(
 *   path = "/data/events",
 *   writeMode = DeltaWriteMode.Append
 * )
 *
 * // Merge (upsert) mode
 * val mergeConfig = DeltaLakeConfig(
 *   path = "/data/events",
 *   writeMode = DeltaWriteMode.Merge,
 *   mergeCondition = Some("source.id = target.id")
 * )
 *
 * // Overwrite with partition filter
 * val overwriteConfig = DeltaLakeConfig(
 *   path = "/data/events",
 *   writeMode = DeltaWriteMode.Overwrite,
 *   replaceWhere = Some("date >= '2024-01-01'")
 * )
 * }}}
 *
 * @param path                 Path to the Delta table (file path or cloud storage URI)
 * @param readChangeDataFeed   Enable Change Data Feed for CDC events (source only)
 * @param startingVersion      Start reading from this table version (source only)
 * @param startingTimestamp    Start reading from this timestamp (source only, ISO-8601 format)
 * @param ignoreChanges        Ignore file changes (re-reads) when streaming (source only)
 * @param ignoreDeletes        Ignore file deletions when streaming (source only)
 * @param maxFilesPerTrigger   Maximum files to process per micro-batch (source only)
 * @param writeMode            Write mode: Append, Merge, or Overwrite (sink only)
 * @param mergeCondition       SQL condition for merge operations (required when writeMode = Merge)
 * @param mergeSchema          Automatically evolve schema on write (sink only)
 * @param partitionBy          Columns to partition by when writing (sink only)
 * @param replaceWhere         SQL predicate for conditional overwrites (sink only)
 * @param schemaEvolutionMode  How to handle schema evolution
 * @param options              Additional Delta Lake options passed to reader/writer
 *
 * @see [[https://docs.delta.io/latest/delta-streaming.html Delta Streaming Guide]]
 * @see [[DeltaWriteMode]] for write mode options
 * @see [[SchemaEvolutionMode]] for schema evolution options
 */
final case class DeltaLakeConfig(
  path: String,
  // Source-specific options
  readChangeDataFeed: Boolean = false,
  startingVersion: Option[Long] = None,
  startingTimestamp: Option[String] = None,
  ignoreChanges: Boolean = false,
  ignoreDeletes: Boolean = false,
  maxFilesPerTrigger: Int = 1000,
  // Sink-specific options
  writeMode: DeltaWriteMode = DeltaWriteMode.Append,
  mergeCondition: Option[String] = None,
  mergeSchema: Boolean = false,
  partitionBy: List[String] = List.empty,
  replaceWhere: Option[String] = None,
  // Shared options
  schemaEvolutionMode: SchemaEvolutionMode = SchemaEvolutionMode.AddColumns,
  options: Map[String, String] = Map.empty) {

  /**
   * Validates the configuration for use as a streaming source.
   *
   * Throws IllegalArgumentException if startingVersion and startingTimestamp are both set.
   */
  def validateForSource(): Unit =
    require(
      startingVersion.isEmpty || startingTimestamp.isEmpty,
      "Cannot specify both startingVersion and startingTimestamp"
    )

  /**
   * Validates the configuration for use as a streaming sink.
   *
   * Throws IllegalArgumentException if merge mode is used without mergeCondition.
   */
  def validateForSink(): Unit =
    writeMode match {
      case DeltaWriteMode.Merge =>
        require(
          mergeCondition.isDefined,
          "mergeCondition is required when writeMode is Merge"
        )
      case _ => // No additional validation needed
    }
}

/** Companion object for [[DeltaLakeConfig]]. */
object DeltaLakeConfig {

  /**
   * Creates a minimal source configuration for reading a Delta table.
   *
   * @param path Path to the Delta table
   * @return A DeltaLakeConfig configured for streaming reads
   */
  def forSource(path: String): DeltaLakeConfig =
    DeltaLakeConfig(path = path)

  /**
   * Creates a source configuration with Change Data Feed enabled.
   *
   * @param path            Path to the Delta table
   * @param startingVersion Optional version to start reading from
   * @return A DeltaLakeConfig configured for CDC streaming
   */
  def forCDC(path: String, startingVersion: Option[Long] = None): DeltaLakeConfig =
    DeltaLakeConfig(
      path = path,
      readChangeDataFeed = true,
      startingVersion = startingVersion
    )

  /**
   * Creates a sink configuration for appending to a Delta table.
   *
   * @param path        Path to the Delta table
   * @param partitionBy Optional partitioning columns
   * @return A DeltaLakeConfig configured for append writes
   */
  def forAppend(path: String, partitionBy: List[String] = List.empty): DeltaLakeConfig =
    DeltaLakeConfig(
      path = path,
      writeMode = DeltaWriteMode.Append,
      partitionBy = partitionBy
    )

  /**
   * Creates a sink configuration for merge (upsert) operations.
   *
   * @param path           Path to the Delta table
   * @param mergeCondition SQL condition for matching rows (e.g., "source.id = target.id")
   * @return A DeltaLakeConfig configured for merge writes
   */
  def forMerge(path: String, mergeCondition: String): DeltaLakeConfig =
    DeltaLakeConfig(
      path = path,
      writeMode = DeltaWriteMode.Merge,
      mergeCondition = Some(mergeCondition)
    )
}
