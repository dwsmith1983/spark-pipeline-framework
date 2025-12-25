package io.github.dwsmith1983.spark.pipeline.config

import io.micrometer.core.instrument.{MeterRegistry, Tags, Timer}

import java.util.concurrent.TimeUnit
import scala.collection.mutable

/**
 * Pipeline lifecycle hooks that record metrics using Micrometer.
 *
 * This class provides production-ready metrics collection for pipeline execution,
 * integrating with Micrometer's vendor-neutral metrics facade. Users can plug in
 * any Micrometer-supported backend (Prometheus, Datadog, CloudWatch, InfluxDB, etc.).
 *
 * == Metrics Recorded ==
 *
 * '''Pipeline-level:'''
 *  - `{prefix}.duration` (Timer) - Total pipeline execution time
 *  - `{prefix}.runs` (Counter) - Number of pipeline executions
 *
 * '''Component-level:'''
 *  - `{prefix}.component.duration` (Timer) - Per-component execution time
 *  - `{prefix}.component.runs` (Counter) - Number of component executions
 *
 * All metrics include `pipeline_name` and `status` tags. Component metrics also
 * include `component_name`.
 *
 * == Example Usage ==
 *
 * {{{
 * import io.micrometer.core.instrument.simple.SimpleMeterRegistry
 *
 * val registry = new SimpleMeterRegistry()
 * val hooks = MetricsHooks(registry)
 *
 * SimplePipelineRunner.run(config, hooks)
 *
 * // Access metrics
 * val duration = registry.get("pipeline.duration").timer()
 * val runs = registry.get("pipeline.runs").counter()
 * }}}
 *
 * @param meterRegistry The Micrometer registry to record metrics to
 * @param metricsPrefix Prefix for all metric names (default: "pipeline")
 * @param commonTags Additional tags to add to all metrics (e.g., environment, service)
 */
class MetricsHooks(
  val meterRegistry: MeterRegistry,
  val metricsPrefix: String = "pipeline",
  val commonTags: Tags = Tags.empty) extends PipelineHooks {

  @volatile private var pipelineStartTime: Long               = 0L
  @volatile private var currentPipelineConfig: PipelineConfig = _
  private val componentStartTimes: mutable.Map[Int, Long]     = mutable.Map()

  override def beforePipeline(config: PipelineConfig): Unit = {
    pipelineStartTime = System.currentTimeMillis()
    currentPipelineConfig = config
    componentStartTimes.clear()
  }

  override def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit = {
    val _ = (config, total)
    componentStartTimes(index) = System.currentTimeMillis()
  }

  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    locally { val _ = total }
    recordComponentMetrics(config.instanceName, durationMs, "success")
    locally { val _ = componentStartTimes.remove(index) }
  }

  override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {
    locally { val _ = error }
    val startTime: Long  = componentStartTimes.getOrElse(index, System.currentTimeMillis())
    val durationMs: Long = System.currentTimeMillis() - startTime
    recordComponentMetrics(config.instanceName, durationMs, "failure")
    locally { val _ = componentStartTimes.remove(index) }
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    val _ = config
    val (durationMs: Long, status: String) = result match {
      case PipelineResult.Success(duration, _) =>
        (duration, "success")
      case PipelineResult.Failure(_, _, _) =>
        val duration: Long = System.currentTimeMillis() - pipelineStartTime
        (duration, "failure")
      case PipelineResult.PartialSuccess(duration, _, _, _) =>
        (duration, "partial_success")
    }

    recordPipelineMetrics(durationMs, status)
  }

  private def recordComponentMetrics(componentName: String, durationMs: Long, status: String): Unit = {
    val pipelineName: String = Option(currentPipelineConfig).map(_.pipelineName).getOrElse("unknown")

    val tags: Tags = Tags.of(
      "pipeline_name",
      pipelineName,
      "component_name",
      componentName,
      "status",
      status
    ).and(commonTags)

    Timer.builder(s"$metricsPrefix.component.duration")
      .tags(tags)
      .register(meterRegistry)
      .record(durationMs, TimeUnit.MILLISECONDS)

    meterRegistry.counter(s"$metricsPrefix.component.runs", tags).increment()
  }

  private def recordPipelineMetrics(durationMs: Long, status: String): Unit = {
    val pipelineName: String = Option(currentPipelineConfig).map(_.pipelineName).getOrElse("unknown")

    val tags: Tags = Tags.of(
      "pipeline_name",
      pipelineName,
      "status",
      status
    ).and(commonTags)

    Timer.builder(s"$metricsPrefix.duration")
      .tags(tags)
      .register(meterRegistry)
      .record(durationMs, TimeUnit.MILLISECONDS)

    meterRegistry.counter(s"$metricsPrefix.runs", tags).increment()
  }
}

/** Factory methods for creating MetricsHooks instances. */
object MetricsHooks {

  /**
   * Creates a MetricsHooks with default settings.
   *
   * @param registry The Micrometer registry to record metrics to
   * @return A new MetricsHooks instance
   */
  def apply(registry: MeterRegistry): MetricsHooks =
    new MetricsHooks(registry)

  /**
   * Creates a MetricsHooks with a custom metrics prefix.
   *
   * @param registry The Micrometer registry to record metrics to
   * @param prefix Custom prefix for all metric names
   * @return A new MetricsHooks instance
   */
  def withPrefix(registry: MeterRegistry, prefix: String): MetricsHooks =
    new MetricsHooks(registry, prefix)

  /**
   * Creates a MetricsHooks with custom prefix and common tags.
   *
   * @param registry The Micrometer registry to record metrics to
   * @param prefix Custom prefix for all metric names
   * @param tags Additional tags to add to all metrics
   * @return A new MetricsHooks instance
   */
  def withTags(registry: MeterRegistry, prefix: String, tags: Tags): MetricsHooks =
    new MetricsHooks(registry, prefix, tags)
}
