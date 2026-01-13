package io.github.dwsmith1983.spark.pipeline.streaming.config

/**
 * Write modes for Apache Iceberg streaming sinks.
 *
 * Determines how data is written to Iceberg tables. Each mode
 * has different performance characteristics and use cases.
 *
 * == Example ==
 *
 * {{{
 * val config = IcebergConfig(
 *   tablePath = "catalog.db.events",
 *   writeMode = IcebergWriteMode.Upsert,
 *   upsertKeys = List("event_id")
 * )
 * }}}
 *
 * @see [[IcebergConfig]] for full Apache Iceberg configuration options
 */
sealed trait IcebergWriteMode

/** Companion object containing Iceberg write mode implementations. */
object IcebergWriteMode {

  /**
   * Append new rows to the table.
   *
   * This is the default and most efficient mode. New data is simply
   * added to the table without affecting existing rows.
   */
  case object Append extends IcebergWriteMode

  /**
   * Upsert (merge) data into the table based on key columns.
   *
   * Requires [[IcebergConfig.upsertKeys]] to be set. Rows with matching
   * keys are updated; rows with new keys are inserted.
   */
  case object Upsert extends IcebergWriteMode

  /**
   * Dynamically overwrite partitions based on the data being written.
   *
   * Only partitions that have data in the current batch are overwritten.
   * Other partitions remain unchanged.
   */
  case object OverwriteDynamic extends IcebergWriteMode
}
