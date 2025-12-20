package io.github.dwsmith1983.spark.pipeline.config

import org.apache.logging.log4j.{LogManager, Logger}

import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.collection.mutable

/**
 * Built-in logging hooks.
 *
 * LoggingHooks provides structured logging for pipeline execution with:
 *   - Correlation ID (`runId`) for tracing all logs from a single execution
 *   - Standardized event names (`pipeline_start`, `component_end`, etc.)
 *   - Duration tracking in milliseconds for performance analysis
 *   - Progress context (`component_index`, `total_components`)
 *   - Two output formats: structured JSON for production, human-readable for development
 *
 * == Usage ==
 *
 * {{{
 * // Structured JSON logging (default)
 * val hooks = new LoggingHooks()
 * SimplePipelineRunner.run(config, hooks)
 *
 * // Human-readable logging for development
 * val devHooks = new LoggingHooks(useStructuredFormat = false)
 *
 * // With external correlation ID
 * val tracedHooks = new LoggingHooks(runId = "trace-abc-123")
 *
 * // Combine with other hooks
 * val combined = PipelineHooks.compose(new LoggingHooks(), metricsHooks)
 * }}}
 *
 * == Structured Log Output ==
 *
 * When `useStructuredFormat = true`, logs are emitted as JSON:
 * {{{
 * {"event":"pipeline_start","run_id":"abc-123","pipeline_name":"WordCount","component_count":3,"timestamp":"..."}
 * {"event":"component_end","run_id":"abc-123","component_name":"Transform","duration_ms":1523,"status":"success"}
 * {"event":"pipeline_end","run_id":"abc-123","duration_ms":5000,"status":"success","components_completed":3}
 * }}}
 *
 * == Human-Readable Output ==
 *
 * When `useStructuredFormat = false`:
 * {{{
 * [INFO] Pipeline 'WordCount' starting (run_id=abc-123, components=3)
 * [INFO] [1/3] Completed 'Transform' in 1523ms
 * [INFO] Pipeline 'WordCount' completed in 5000ms (run_id=abc-123)
 * }}}
 *
 * @param runId Correlation ID for this pipeline execution. Defaults to auto-generated UUID.
 * @param useStructuredFormat If true, emit JSON logs. If false, emit human-readable logs.
 */
class LoggingHooks(
  val runId: String = java.util.UUID.randomUUID().toString.take(8),
  val useStructuredFormat: Boolean = true) extends PipelineHooks {

  private val logger: Logger = LogManager.getLogger(classOf[LoggingHooks])

  @volatile private var pipelineStartTime: Long           = 0L
  private val componentStartTimes: mutable.Map[Int, Long] = mutable.Map()

  private def timestamp: String = DateTimeFormatter.ISO_INSTANT.format(Instant.now())

  private def escapeJson(s: String): String =
    s.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")

  override def beforePipeline(config: PipelineConfig): Unit = {
    pipelineStartTime = System.currentTimeMillis()
    val componentCount = config.pipelineComponents.size

    if (useStructuredFormat) {
      logger.info(
        s"""{"event":"pipeline_start","run_id":"$runId","pipeline_name":"${escapeJson(
            config.pipelineName
          )}","component_count":$componentCount,"timestamp":"$timestamp"}"""
      )
    } else {
      logger.info(
        s"Pipeline '${config.pipelineName}' starting (run_id=$runId, components=$componentCount)"
      )
    }
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    val (status, durationMs, componentsCompleted) = result match {
      case PipelineResult.Success(duration, completed) =>
        ("success", duration, completed)
      case PipelineResult.Failure(_, _, completed) =>
        ("failure", System.currentTimeMillis() - pipelineStartTime, completed)
    }

    if (useStructuredFormat) {
      logger.info(
        s"""{"event":"pipeline_end","run_id":"$runId","pipeline_name":"${escapeJson(
            config.pipelineName
          )}","duration_ms":$durationMs,"status":"$status","components_completed":$componentsCompleted,"timestamp":"$timestamp"}"""
      )
    } else {
      val statusText = if (status == "success") "completed" else "failed"
      logger.info(
        s"Pipeline '${config.pipelineName}' $statusText in ${durationMs}ms (run_id=$runId, completed=$componentsCompleted)"
      )
    }
  }

  override def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit = {
    componentStartTimes(index) = System.currentTimeMillis()
    val componentNumber = index + 1

    if (useStructuredFormat) {
      logger.info(
        s"""{"event":"component_start","run_id":"$runId","component_name":"${escapeJson(
            config.instanceName
          )}","component_type":"${escapeJson(
            config.instanceType
          )}","component_index":$componentNumber,"total_components":$total,"timestamp":"$timestamp"}"""
      )
    } else {
      logger.info(s"[$componentNumber/$total] Starting '${config.instanceName}'")
    }
  }

  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    componentStartTimes.remove(index)
    val componentNumber = index + 1

    if (useStructuredFormat) {
      logger.info(
        s"""{"event":"component_end","run_id":"$runId","component_name":"${escapeJson(
            config.instanceName
          )}","component_index":$componentNumber,"total_components":$total,"duration_ms":$durationMs,"status":"success","timestamp":"$timestamp"}"""
      )
    } else {
      logger.info(s"[$componentNumber/$total] Completed '${config.instanceName}' in ${durationMs}ms")
    }
  }

  override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {
    val componentNumber = index + 1
    val errorType       = error.getClass.getSimpleName
    val errorMessage    = Option(error.getMessage).getOrElse("No message")
    val durationMs = componentStartTimes.get(index) match {
      case Some(startTime) => System.currentTimeMillis() - startTime
      case None            => 0L
    }
    componentStartTimes.remove(index)

    if (useStructuredFormat) {
      logger.error(
        s"""{"event":"component_error","run_id":"$runId","component_name":"${escapeJson(
            config.instanceName
          )}","component_index":$componentNumber,"duration_ms":$durationMs,"error_type":"$errorType","error_message":"${escapeJson(
            errorMessage
          )}","timestamp":"$timestamp"}"""
      )
    } else {
      logger.error(
        s"[$componentNumber] Component '${config.instanceName}' failed after ${durationMs}ms: $errorType - $errorMessage"
      )
    }
  }
}

object LoggingHooks {

  /**
   * Creates a LoggingHooks instance with structured JSON format.
   *
   * @param runId Optional correlation ID. Auto-generates if not provided.
   * @return LoggingHooks configured for structured JSON output
   */
  def structured(runId: String = java.util.UUID.randomUUID().toString.take(8)): LoggingHooks =
    new LoggingHooks(runId = runId, useStructuredFormat = true)

  /**
   * Creates a LoggingHooks instance with human-readable format.
   *
   * @param runId Optional correlation ID. Auto-generates if not provided.
   * @return LoggingHooks configured for human-readable output
   */
  def humanReadable(runId: String = java.util.UUID.randomUUID().toString.take(8)): LoggingHooks =
    new LoggingHooks(runId = runId, useStructuredFormat = false)
}
