package io.github.dwsmith1983.pipelines

import io.github.dwsmith1983.spark.pipeline.config._

import scala.collection.mutable

/**
 * Example PipelineHooks implementation for collecting execution metrics.
 *
 * This demonstrates a practical use case for lifecycle hooks:
 * - Tracking component execution times
 * - Recording success/failure counts
 * - Generating pipeline execution reports
 *
 * == Usage ==
 *
 * {{{
 * val metrics = new MetricsHooks()
 * SimplePipelineRunner.run(config, metrics)
 *
 * // After execution, access the collected metrics
 * println(s"Total duration: $${metrics.totalDurationMs}ms")
 * println(s"Components run: $${metrics.componentCount}")
 * metrics.componentDurations.foreach { case (name, duration) =>
 *   println(s"  $$name: $${duration}ms")
 * }
 * }}}
 *
 * == Composing with Other Hooks ==
 *
 * {{{
 * val composed = PipelineHooks.compose(
 *   new MetricsHooks(),
 *   new AlertingHooks(slackWebhook)
 * )
 * SimplePipelineRunner.run(config, composed)
 * }}}
 */
class MetricsHooks extends PipelineHooks {

  // Collected metrics
  private var _pipelineName: String                                    = _
  private var _pipelineStartTime: Long                                 = 0L
  private var _totalDurationMs: Long                                   = 0L
  private var _componentCount: Int                                     = 0
  private var _successCount: Int                                       = 0
  private var _failureCount: Int                                       = 0
  private val _componentDurations: mutable.LinkedHashMap[String, Long] = mutable.LinkedHashMap.empty
  private var _failedComponent: Option[String]                         = None
  private var _failureError: Option[Throwable]                         = None

  // Accessors for collected metrics
  def pipelineName: String                  = _pipelineName
  def totalDurationMs: Long                 = _totalDurationMs
  def componentCount: Int                   = _componentCount
  def successCount: Int                     = _successCount
  def failureCount: Int                     = _failureCount
  def componentDurations: Map[String, Long] = _componentDurations.toMap
  def failedComponent: Option[String]       = _failedComponent
  def failureError: Option[Throwable]       = _failureError
  def wasSuccessful: Boolean                = _failureCount == 0

  override def beforePipeline(config: PipelineConfig): Unit = {
    _pipelineName = config.pipelineName
    _pipelineStartTime = System.currentTimeMillis()
    _componentCount = config.pipelineComponents.size
  }

  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    _componentDurations(config.instanceName) = durationMs
    _successCount += 1
  }

  override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {
    _failedComponent = Some(config.instanceName)
    _failureError = Some(error)
    _failureCount += 1
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    val _ = config // suppress unused warning
    _totalDurationMs = result match {
      case PipelineResult.Success(duration, _)              => duration
      case PipelineResult.Failure(_, _, _)                  => System.currentTimeMillis() - _pipelineStartTime
      case PipelineResult.PartialSuccess(duration, _, _, _) => duration
    }
  }

  /**
   * Generates a formatted report of the pipeline execution.
   *
   * @return Multi-line string with execution summary
   */
  def generateReport(): String = {
    val sb = new StringBuilder()
    sb.append(s"Pipeline: ${_pipelineName}\n")
    sb.append(s"Status: ${if (wasSuccessful) "SUCCESS" else "FAILED"}\n")
    sb.append(s"Total Duration: ${_totalDurationMs}ms\n")
    sb.append(s"Components: ${_successCount}/${_componentCount} completed\n")

    if (_componentDurations.nonEmpty) {
      sb.append("\nComponent Timings:\n")
      _componentDurations.foreach {
        case (name, duration) =>
          val pct = if (_totalDurationMs > 0) duration * 100.0 / _totalDurationMs else 0.0
          sb.append(f"  $name: ${duration}ms ($pct%.1f%%)\n")
      }
    }

    _failedComponent.foreach { name =>
      sb.append(s"\nFailed Component: $name\n")
      _failureError.foreach(error => sb.append(s"Error: ${error.getMessage}\n"))
    }

    sb.toString()
  }

  /** Resets all collected metrics for reuse. */
  def reset(): Unit = {
    _pipelineName = null
    _pipelineStartTime = 0L
    _totalDurationMs = 0L
    _componentCount = 0
    _successCount = 0
    _failureCount = 0
    _componentDurations.clear()
    _failedComponent = None
    _failureError = None
  }
}

/** Companion object with a pre-built instance for simple usage. */
object MetricsHooks {

  /** Creates a new MetricsHooks instance. */
  def apply(): MetricsHooks = new MetricsHooks()
}
