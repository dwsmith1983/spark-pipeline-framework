package io.github.dwsmith1983.spark.pipeline.streaming

/**
 * Configuration for Spark Structured Streaming triggers.
 *
 * Triggers control how often a streaming query produces results. This
 * configuration model is Spark-independent and is converted to actual
 * Spark Trigger objects at runtime.
 *
 * == Supported Trigger Types ==
 *
 * '''ProcessingTime''': Execute micro-batches at fixed intervals.
 * Use this for most streaming workloads.
 *
 * '''Once''': Execute exactly one micro-batch and stop. Useful for
 * batch-like streaming or testing.
 *
 * '''Continuous''': Experimental low-latency continuous processing mode.
 * Only supported by certain sources and sinks.
 *
 * '''AvailableNow''': Process all available data in multiple batches,
 * then stop. Added in Spark 3.3+.
 *
 * == Example ==
 *
 * {{{
 * // Process every 10 seconds
 * val trigger = TriggerConfig.ProcessingTime("10 seconds")
 *
 * // Run once for testing
 * val onceTrigger = TriggerConfig.Once
 *
 * // Low-latency continuous processing
 * val continuous = TriggerConfig.Continuous("1 second")
 * }}}
 *
 * @see [[https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers Spark Triggers]]
 */
sealed trait TriggerConfig {

  /**
   * Human-readable description of this trigger configuration.
   *
   * @return A string describing the trigger type and parameters
   */
  def description: String
}

/** Companion object containing trigger configuration implementations. */
object TriggerConfig {

  /**
   * Execute streaming micro-batches at a fixed interval.
   *
   * This is the most common trigger type for streaming applications.
   * If the previous micro-batch takes longer than the interval, the
   * next batch starts immediately after the previous one completes.
   *
   * @param interval The interval between micro-batches (e.g., "10 seconds", "1 minute")
   */
  final case class ProcessingTime(interval: String) extends TriggerConfig {
    override def description: String = s"ProcessingTime($interval)"
  }

  /**
   * Execute exactly one micro-batch, then stop the query.
   *
   * Useful for:
   * - Testing streaming pipelines
   * - Batch-style processing with streaming semantics
   * - One-time data migrations
   */
  case object Once extends TriggerConfig {
    override def description: String = "Once"
  }

  /**
   * Experimental continuous processing mode with low latency.
   *
   * This mode processes data continuously with minimal latency (~1ms),
   * but has limited source/sink support and requires at-least-once
   * semantics for some scenarios.
   *
   * @param checkpointInterval Interval for asynchronous checkpointing
   */
  final case class Continuous(checkpointInterval: String) extends TriggerConfig {
    override def description: String = s"Continuous($checkpointInterval)"
  }

  /**
   * Process all available data, then stop.
   *
   * Similar to Once, but processes data in multiple batches if there's
   * a large backlog. This is useful for catching up on historical data
   * before switching to regular streaming.
   *
   * @note Requires Spark 3.3 or later
   */
  case object AvailableNow extends TriggerConfig {
    override def description: String = "AvailableNow"
  }
}
