package io.github.dwsmith1983.spark.pipeline.streaming.config

/**
 * Processing mode for foreach sink.
 *
 * Determines whether processing happens at the batch level (foreachBatch)
 * or row level (foreach).
 */
sealed trait ForeachMode

object ForeachMode {

  /**
   * Process data at the batch level using foreachBatch.
   *
   * Provides a DataFrame for each micro-batch, allowing arbitrary
   * transformations and writes. Best for database upserts, API calls
   * with batching, or complex processing logic.
   */
  case object Batch extends ForeachMode

  /**
   * Process data at the row level using foreach.
   *
   * Provides a ForeachWriter interface for row-by-row processing.
   * Best for custom serialization, external system integration,
   * or when batch processing is not suitable.
   */
  case object Row extends ForeachMode

  /**
   * Parse a foreach mode from string.
   *
   * @param s Mode string (case-insensitive): batch or row
   * @return The corresponding ForeachMode
   */
  def fromString(s: String): ForeachMode = s.toLowerCase match {
    case "batch" => Batch
    case "row"   => Row
    case other =>
      throw new IllegalArgumentException(
        s"Unknown foreach mode: '$other'. Supported: batch, row"
      )
  }
}

/**
 * Configuration for custom foreach streaming sink.
 *
 * This sink allows custom processing logic by specifying a processor
 * class that will be instantiated and invoked for each micro-batch
 * or row, depending on the mode.
 *
 * == Processing Modes ==
 *
 * '''Batch mode''' (default): Uses `foreachBatch` to process entire
 * micro-batches. The processor class must extend `StreamBatchProcessor`.
 *
 * '''Row mode''': Uses `foreach` to process individual rows. The
 * processor class must extend `StreamRowProcessor`.
 *
 * == Processor Interfaces ==
 *
 * {{{
 * // For batch mode
 * trait StreamBatchProcessor extends Serializable {
 *   def processBatch(df: DataFrame, batchId: Long): Unit
 *   def onQueryTermination(): Unit = {}
 * }
 *
 * // For row mode
 * trait StreamRowProcessor extends Serializable {
 *   def open(partitionId: Long, epochId: Long): Boolean
 *   def process(row: Row): Unit
 *   def close(error: Throwable): Unit
 * }
 * }}}
 *
 * == Example HOCON ==
 *
 * {{{
 * instance-config {
 *   processor-class = "com.example.MyBatchProcessor"
 *   checkpoint-path = "/checkpoints/foreach-sink"
 *   mode = "batch"
 *   processor-config {
 *     database-url = "jdbc:postgresql://localhost/db"
 *     batch-size = 1000
 *   }
 * }
 * }}}
 *
 * @param processorClass Fully qualified class name of the processor
 * @param checkpointPath Path for Spark checkpoint storage
 * @param mode Processing mode: batch (foreachBatch) or row (foreach)
 * @param queryName Optional name for the streaming query
 * @param processorConfig Configuration to pass to the processor
 */
case class ForeachSinkConfig(
  processorClass: String,
  checkpointPath: String,
  mode: String = "batch",
  queryName: Option[String] = None,
  processorConfig: Map[String, String] = Map.empty) {

  /** Get the parsed foreach mode. */
  def foreachMode: ForeachMode = ForeachMode.fromString(mode)
}
