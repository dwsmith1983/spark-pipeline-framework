package io.github.dwsmith1983.spark.pipeline.streaming

import org.apache.spark.sql.streaming.Trigger

import scala.annotation.nowarn

/**
 * Converts `TriggerConfig` to Spark `Trigger` objects.
 *
 * This converter bridges the Spark-independent configuration model
 * in the core module with the actual Spark Trigger instances needed
 * at runtime.
 *
 * == Example ==
 *
 * {{{
 * val config = TriggerConfig.ProcessingTime("10 seconds")
 * val trigger = TriggerConverter.toSparkTrigger(config)
 * // trigger is Trigger.ProcessingTime(10.seconds)
 * }}}
 */
object TriggerConverter {

  /**
   * Converts a TriggerConfig to a Spark Trigger.
   *
   * @param config The trigger configuration
   * @return The corresponding Spark Trigger instance
   */
  @nowarn("msg=deprecated")
  def toSparkTrigger(config: TriggerConfig): Trigger = config match {
    case TriggerConfig.ProcessingTime(interval) =>
      Trigger.ProcessingTime(interval)

    case TriggerConfig.Once =>
      Trigger.Once()

    case TriggerConfig.Continuous(checkpointInterval) =>
      Trigger.Continuous(checkpointInterval)

    case TriggerConfig.AvailableNow =>
      Trigger.AvailableNow()
  }
}
