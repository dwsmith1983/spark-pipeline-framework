package io.github.dwsmith1983.spark.pipeline.streaming.config

/**
 * Write modes for Delta Lake streaming sinks.
 *
 * Determines how data is written to Delta Lake tables. Each mode
 * has different semantics and requirements.
 *
 * == Example ==
 *
 * {{{
 * val config = DeltaLakeConfig(
 *   path = "/data/events",
 *   writeMode = DeltaWriteMode.Merge,
 *   mergeCondition = Some("source.id = target.id")
 * )
 * }}}
 *
 * @see [[DeltaLakeConfig]] for full Delta Lake configuration options
 */
sealed trait DeltaWriteMode

/** Companion object containing Delta write mode implementations. */
object DeltaWriteMode {

  /**
   * Append new rows to the table.
   *
   * This is the default mode. New data is simply added to the table
   * without affecting existing rows.
   */
  case object Append extends DeltaWriteMode

  /**
   * Merge (upsert) data into the table.
   *
   * Requires [[DeltaLakeConfig.mergeCondition]] to be set. Rows that
   * match the condition are updated; non-matching rows are inserted.
   */
  case object Merge extends DeltaWriteMode

  /**
   * Overwrite data in the table.
   *
   * If [[DeltaLakeConfig.replaceWhere]] is set, only matching partitions
   * are overwritten. Otherwise, the entire table is replaced.
   */
  case object Overwrite extends DeltaWriteMode
}
