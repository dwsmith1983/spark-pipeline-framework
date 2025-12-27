package io.github.dwsmith1983.spark.pipeline.audit

import io.github.dwsmith1983.spark.pipeline.config._

import java.io.{PrintWriter, StringWriter}
import java.time.Instant
import scala.collection.mutable

/**
 * Pipeline lifecycle hooks that write audit events to a sink.
 *
 * Provides comprehensive audit trails for pipeline executions including:
 *   - Pipeline start/end events with full context
 *   - Component start/end/failure events
 *   - System context (JVM, hostname, environment)
 *   - Optional Spark context (application ID, master, config)
 *   - Full component configuration snapshots (with sensitive value filtering)
 *
 * == Basic Usage ==
 *
 * {{{
 * // File-based audit with defaults
 * val hooks = AuditHooks.toFile("/var/log/pipeline-audit.jsonl")
 * SimplePipelineRunner.run(config, hooks)
 * }}}
 *
 * == With Spark Context Enrichment ==
 *
 * {{{
 * // From runtime module
 * import io.github.dwsmith1983.spark.pipeline.audit.SparkAuditContextProvider
 *
 * val hooks = AuditHooks(
 *   sink = AuditSink.file("/var/log/pipeline-audit.jsonl"),
 *   contextProvider = SparkAuditContextProvider()
 * )
 * }}}
 *
 * == Custom Filtering ==
 *
 * {{{
 * val hooks = AuditHooks(
 *   sink = AuditSink.file("/var/log/pipeline-audit.jsonl"),
 *   configFilter = ConfigFilter.withAdditionalPatterns(Set("mycompany_secret")),
 *   envFilter = EnvFilter.withAdditionalAllowedKeys(Set("MY_APP_ENV"))
 * )
 * }}}
 *
 * == Combining with Other Hooks ==
 *
 * {{{
 * val combined = PipelineHooks.compose(
 *   AuditHooks.toFile("/var/log/audit.jsonl"),
 *   MetricsHooks(registry),
 *   LoggingHooks.structured()
 * )
 * SimplePipelineRunner.run(config, combined)
 * }}}
 *
 * @param sink The sink to write audit events to
 * @param runId Correlation ID for all events in this run (auto-generated if not provided)
 * @param contextProvider Provider for system and Spark context
 * @param configFilter Filter for component configuration values
 * @param envFilter Filter for environment variables
 * @param includeStackTrace Whether to include stack traces in failure events
 */
class AuditHooks(
  val sink: AuditSink,
  val runId: String = java.util.UUID.randomUUID().toString,
  val contextProvider: AuditContextProvider = AuditContextProvider.default,
  val configFilter: ConfigFilter = ConfigFilter.default,
  val envFilter: EnvFilter = EnvFilter.default,
  val includeStackTrace: Boolean = true)
  extends PipelineHooks {

  @volatile private var pipelineStartTime: Long               = 0L
  @volatile private var currentPipelineConfig: PipelineConfig = _
  private val componentStartTimes: mutable.Map[Int, Long]     = mutable.Map()

  private def generateEventId(): String =
    java.util.UUID.randomUUID().toString.take(16)

  private def getSystemContext(): SystemContext =
    contextProvider.getSystemContext(envFilter)

  private def getSparkContext(): Option[SparkExecutionContext] =
    try {
      contextProvider.getSparkContext()
    } catch {
      case _: Exception => None
    }

  private def getPipelineName(): String =
    Option(currentPipelineConfig).map(_.pipelineName).getOrElse("unknown")

  override def beforePipeline(config: PipelineConfig): Unit = {
    pipelineStartTime = System.currentTimeMillis()
    currentPipelineConfig = config
    componentStartTimes.clear()

    val event = PipelineStartEvent(
      eventId = generateEventId(),
      runId = runId,
      timestamp = Instant.now(),
      pipelineName = config.pipelineName,
      componentCount = config.pipelineComponents.size,
      failFast = config.failFast,
      systemContext = getSystemContext(),
      sparkContext = getSparkContext()
    )

    sink.write(event)
  }

  override def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit = {
    componentStartTimes(index) = System.currentTimeMillis()

    val event = ComponentStartEvent(
      eventId = generateEventId(),
      runId = runId,
      timestamp = Instant.now(),
      pipelineName = getPipelineName(),
      componentName = config.instanceName,
      componentType = config.instanceType,
      componentIndex = index + 1,
      totalComponents = total,
      componentConfig = configFilter.filter(config.instanceConfig),
      systemContext = getSystemContext(),
      sparkContext = getSparkContext()
    )

    sink.write(event)
  }

  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    locally { val _ = componentStartTimes.remove(index) }

    val event = ComponentEndEvent(
      eventId = generateEventId(),
      runId = runId,
      timestamp = Instant.now(),
      pipelineName = getPipelineName(),
      componentName = config.instanceName,
      componentType = config.instanceType,
      componentIndex = index + 1,
      totalComponents = total,
      durationMs = durationMs,
      status = "success",
      systemContext = getSystemContext(),
      sparkContext = getSparkContext()
    )

    sink.write(event)
  }

  override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {
    val startTime  = componentStartTimes.getOrElse(index, System.currentTimeMillis())
    val durationMs = System.currentTimeMillis() - startTime
    locally { val _ = componentStartTimes.remove(index) }

    val stackTrace = if (includeStackTrace) {
      val sw = new StringWriter()
      error.printStackTrace(new PrintWriter(sw))
      Some(sw.toString.take(4000)) // Limit stack trace length
    } else None

    val event = ComponentFailureEvent(
      eventId = generateEventId(),
      runId = runId,
      timestamp = Instant.now(),
      pipelineName = getPipelineName(),
      componentName = config.instanceName,
      componentType = config.instanceType,
      componentIndex = index + 1,
      durationMs = durationMs,
      errorType = error.getClass.getSimpleName,
      errorMessage = Option(error.getMessage).getOrElse("No message"),
      stackTrace = stackTrace,
      systemContext = getSystemContext(),
      sparkContext = getSparkContext()
    )

    sink.write(event)
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    val (status, durationMs, completed, failed, errorInfo) = result match {
      case PipelineResult.Success(duration, ran) =>
        ("success", duration, ran, 0, (None, None))
      case PipelineResult.Failure(error, _, ran) =>
        val duration = System.currentTimeMillis() - pipelineStartTime
        ("failure", duration, ran, 1, (Some(error.getClass.getSimpleName), Option(error.getMessage)))
      case PipelineResult.PartialSuccess(duration, succeeded, failedCount, _) =>
        ("partial_success", duration, succeeded, failedCount, (None, None))
    }

    val event = PipelineEndEvent(
      eventId = generateEventId(),
      runId = runId,
      timestamp = Instant.now(),
      pipelineName = config.pipelineName,
      durationMs = durationMs,
      status = status,
      componentsCompleted = completed,
      componentsFailed = failed,
      errorType = errorInfo._1,
      errorMessage = errorInfo._2,
      systemContext = getSystemContext(),
      sparkContext = getSparkContext()
    )

    sink.write(event)
    sink.flush() // Ensure all events are persisted
  }
}

/** Factory methods for creating AuditHooks instances. */
object AuditHooks {

  /**
   * Creates AuditHooks with a file sink.
   *
   * @param path Path to the audit log file (JSON lines format)
   * @return AuditHooks configured to write to the file
   */
  def toFile(path: String): AuditHooks =
    new AuditHooks(AuditSink.file(path))

  /**
   * Creates AuditHooks with a file sink and custom run ID.
   *
   * @param path Path to the audit log file
   * @param runId Correlation ID for all events
   * @return AuditHooks configured with the specified run ID
   */
  def toFile(path: String, runId: String): AuditHooks =
    new AuditHooks(AuditSink.file(path), runId = runId)

  /**
   * Creates AuditHooks with a custom sink.
   *
   * @param sink The sink to write events to
   * @return AuditHooks configured with the sink
   */
  def apply(sink: AuditSink): AuditHooks =
    new AuditHooks(sink)

  /**
   * Creates AuditHooks with full customization.
   *
   * @param sink The sink to write events to
   * @param runId Correlation ID for all events
   * @param contextProvider Provider for system and Spark context
   * @param configFilter Filter for component configuration values
   * @param envFilter Filter for environment variables
   * @param includeStackTrace Whether to include stack traces in failure events
   * @return Fully customized AuditHooks
   */
  def apply(
    sink: AuditSink,
    runId: String = java.util.UUID.randomUUID().toString,
    contextProvider: AuditContextProvider = AuditContextProvider.default,
    configFilter: ConfigFilter = ConfigFilter.default,
    envFilter: EnvFilter = EnvFilter.default,
    includeStackTrace: Boolean = true
  ): AuditHooks = new AuditHooks(sink, runId, contextProvider, configFilter, envFilter, includeStackTrace)
}
