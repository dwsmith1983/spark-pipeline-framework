# Lifecycle Hooks

Lifecycle hooks allow you to monitor pipeline execution, collect metrics, handle errors, and integrate with external systems without modifying your components.

## Built-in LoggingHooks

The framework includes a production-ready `LoggingHooks` implementation with SRE best practices:

```scala
import io.github.dwsmith1983.spark.pipeline.config.LoggingHooks

// Structured JSON logging (recommended for production)
val hooks = new LoggingHooks()
SimplePipelineRunner.run(config, hooks)

// Human-readable logging (for development)
val devHooks = LoggingHooks.humanReadable()

// With external correlation ID for distributed tracing
val tracedHooks = new LoggingHooks(runId = "trace-abc-123")
```

### Structured Log Output

When using structured format (default), logs are emitted as JSON for easy parsing by log aggregators (Splunk, ELK, Datadog):

```json
{"event":"pipeline_start","run_id":"abc-123","pipeline_name":"WordCount","component_count":3,"timestamp":"..."}
{"event":"component_start","run_id":"abc-123","component_name":"ReadInput","component_index":1,"total_components":3}
{"event":"component_end","run_id":"abc-123","component_name":"ReadInput","duration_ms":1523,"status":"success"}
{"event":"pipeline_end","run_id":"abc-123","duration_ms":5000,"status":"success","components_completed":3}
```

### Human-Readable Output

For development, use `LoggingHooks.humanReadable()`:

```
[INFO] Pipeline 'WordCount' starting (run_id=abc-123, components=3)
[INFO] [1/3] Starting 'ReadInput'
[INFO] [1/3] Completed 'ReadInput' in 1523ms
[INFO] Pipeline 'WordCount' completed in 5000ms (run_id=abc-123)
```

### Features

- **Correlation ID**: Every log includes `run_id` for filtering a single execution
- **Standardized Events**: `pipeline_start`, `component_end`, `component_error`, etc.
- **Duration Tracking**: `duration_ms` for performance analysis (p50, p95, p99)
- **Progress Context**: `component_index` and `total_components`
- **Error Classification**: `error_type` and `error_message` fields

## Built-in MetricsHooks

The framework includes production-ready `MetricsHooks` that integrate with [Micrometer](https://micrometer.io/), a vendor-neutral metrics facade. This allows you to plug in any supported backend: Prometheus, Datadog, CloudWatch, InfluxDB, and more.

```scala
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.github.dwsmith1983.spark.pipeline.config.MetricsHooks

// Create with any Micrometer registry
val registry = new SimpleMeterRegistry()  // or PrometheusMeterRegistry, etc.
val hooks = MetricsHooks(registry)

SimplePipelineRunner.run(config, hooks)

// Access metrics after execution
val pipelineDuration = registry.get("pipeline.duration").timer()
val componentRuns = registry.get("pipeline.component.runs").counter()
```

### Available Metrics

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `pipeline.duration` | Timer | pipeline_name, status | Total pipeline execution time |
| `pipeline.runs` | Counter | pipeline_name, status | Number of pipeline executions |
| `pipeline.component.duration` | Timer | pipeline_name, component_name, status | Per-component execution time |
| `pipeline.component.runs` | Counter | pipeline_name, component_name, status | Component execution count |

### Status Tags

- `success` - Completed without error
- `failure` - Failed with exception
- `partial_success` - Some components failed (when `failFast=false`)

### Custom Prefix and Tags

```scala
import io.micrometer.core.instrument.Tags

// Custom metrics prefix
val hooks = MetricsHooks.withPrefix(registry, "myapp.pipeline")

// Add common tags (e.g., environment, service name)
val hooks = MetricsHooks.withTags(
  registry,
  "pipeline",
  Tags.of("env", "production", "service", "etl")
)
```

### Prometheus Example

```scala
import io.micrometer.prometheus.{PrometheusConfig, PrometheusMeterRegistry}

val prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
val hooks = MetricsHooks(prometheusRegistry)

SimplePipelineRunner.run(config, hooks)

// Expose metrics endpoint
println(prometheusRegistry.scrape())  // Returns Prometheus format
```

## PipelineHooks Trait

For custom hooks, implement the `PipelineHooks` trait:

```scala
import io.github.dwsmith1983.spark.pipeline.config._

trait PipelineHooks {
  def beforePipeline(config: PipelineConfig): Unit = {}
  def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {}
  def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit = {}
  def afterComponent(config: ComponentConfig, index: Int, total: Int, durationMs: Long): Unit = {}
  def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {}
}
```

## Pipeline Result

The `PipelineResult` sealed trait represents pipeline completion status:

```scala
sealed trait PipelineResult {
  def isSuccess: Boolean
  def isFailure: Boolean = !isSuccess
}

object PipelineResult {
  case class Success(
    durationMs: Long,
    componentsRun: Int
  ) extends PipelineResult

  case class Failure(
    error: Throwable,
    failedComponent: Option[ComponentConfig],
    componentsCompleted: Int
  ) extends PipelineResult

  case class PartialSuccess(
    durationMs: Long,
    componentsSucceeded: Int,
    componentsFailed: Int,
    failures: List[(ComponentConfig, Throwable)]
  ) extends PipelineResult  // isSuccess = false
}
```

`PartialSuccess` is returned when `fail-fast = false` and some components fail. It contains details of all failures while allowing you to see which components succeeded.

## Basic Example

```scala
import io.github.dwsmith1983.spark.pipeline.config._

val loggingHooks = new PipelineHooks {
  override def beforePipeline(config: PipelineConfig): Unit =
    println(s"Starting pipeline: ${config.pipelineName}")

  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit =
    println(s"[${index + 1}/$total] ${config.instanceName} completed in ${durationMs}ms")

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit =
    result match {
      case PipelineResult.Success(duration, count) =>
        println(s"Pipeline completed: $count components in ${duration}ms")
      case PipelineResult.Failure(error, _, name) =>
        println(s"Pipeline failed at $name: ${error.getMessage}")
    }
}

// Use with SimplePipelineRunner
SimplePipelineRunner.run(config, loggingHooks)
```

## Custom Metrics Collection Example

For simple use cases without external metrics systems, you can implement a custom hooks class.
For production, use the built-in [MetricsHooks](#built-in-metricshooks) with Micrometer.

```scala
import scala.collection.mutable

class CustomMetricsHooks extends PipelineHooks {
  private val componentTimings = mutable.LinkedHashMap[String, Long]()
  private var pipelineStartTime: Long = 0L
  private var pipelineResult: Option[PipelineResult] = None

  override def beforePipeline(config: PipelineConfig): Unit = {
    pipelineStartTime = System.currentTimeMillis()
    componentTimings.clear()
  }

  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    componentTimings(config.instanceName) = durationMs
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    pipelineResult = Some(result)
  }

  def printReport(): Unit = {
    val totalTime = componentTimings.values.sum
    println("\n" + "=" * 60)
    println("METRICS REPORT")
    println("=" * 60)
    println(s"Status: ${if (pipelineResult.exists(_.isInstanceOf[PipelineResult.Success])) "SUCCESS" else "FAILED"}")
    println(s"Total Duration: ${totalTime}ms")
    println("\nComponent Timings:")
    componentTimings.foreach { case (name, duration) =>
      val percentage = (duration.toDouble / totalTime * 100).formatted("%.1f")
      println(s"  $name: ${duration}ms ($percentage%)")
    }
    println("=" * 60)
  }
}
```

## Composing Hooks

Combine multiple hooks using `PipelineHooks.compose()`:

```scala
val loggingHooks = new PipelineHooks { /* ... */ }
val metricsHooks = new MetricsHooks()
val alertingHooks = new PipelineHooks { /* ... */ }

// All hooks will be called in order
val composedHooks = PipelineHooks.compose(loggingHooks, metricsHooks, alertingHooks)

SimplePipelineRunner.run(config, composedHooks)
```

Hooks are called in the order they are passed to `compose()`.

## Use Cases

### Slack/Email Alerts

```scala
class AlertingHooks(slackWebhook: String) extends PipelineHooks {
  override def onComponentFailure(
    config: ComponentConfig,
    index: Int,
    error: Throwable
  ): Unit = {
    sendSlackMessage(
      slackWebhook,
      s":x: Component ${config.instanceName} failed: ${error.getMessage}"
    )
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    result match {
      case PipelineResult.Success(duration, _) =>
        sendSlackMessage(slackWebhook, s":white_check_mark: ${config.pipelineName} completed in ${duration}ms")
      case PipelineResult.Failure(error, _, name) =>
        sendSlackMessage(slackWebhook, s":fire: ${config.pipelineName} failed at $name")
    }
  }

  private def sendSlackMessage(webhook: String, message: String): Unit = {
    // HTTP POST to Slack webhook
  }
}
```

### Datadog/Prometheus Metrics

```scala
class DatadogHooks(statsdClient: StatsDClient) extends PipelineHooks {
  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    statsdClient.histogram(
      "pipeline.component.duration",
      durationMs,
      s"component:${config.instanceName}"
    )
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    val status = result match {
      case _: PipelineResult.Success => "success"
      case _: PipelineResult.Failure => "failure"
    }
    statsdClient.increment(s"pipeline.completion.$status", s"pipeline:${config.pipelineName}")
  }
}
```

### Audit Logging

```scala
class AuditHooks(auditLogger: Logger) extends PipelineHooks {
  override def beforePipeline(config: PipelineConfig): Unit = {
    auditLogger.info(Map(
      "event" -> "pipeline_started",
      "pipeline" -> config.pipelineName,
      "components" -> config.pipelineComponents.size,
      "timestamp" -> Instant.now().toString
    ))
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    auditLogger.info(Map(
      "event" -> "pipeline_completed",
      "pipeline" -> config.pipelineName,
      "status" -> result.getClass.getSimpleName,
      "timestamp" -> Instant.now().toString
    ))
  }
}
```

## Running with Hooks

### Programmatic Usage

```scala
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner
import com.typesafe.config.ConfigFactory

val config = ConfigFactory.load("pipeline.conf")
val hooks = PipelineHooks.compose(loggingHooks, metricsHooks)

SimplePipelineRunner.run(config, hooks)
```

### Default (No-Op) Hooks

If you don't need hooks, use the default no-op implementation:

```scala
SimplePipelineRunner.run(config)  // Uses PipelineHooks.NoOp internally
```

## Best Practices

1. **Keep hooks lightweight** - Don't do heavy processing in hooks; they run synchronously
2. **Handle exceptions** - Wrap hook logic in try-catch to prevent hook failures from affecting the pipeline
3. **Use composition** - Split concerns into separate hook implementations and compose them
4. **Async for I/O** - For network calls (Slack, metrics), consider async to avoid blocking

```scala
class SafeAlertingHooks extends PipelineHooks {
  override def onComponentFailure(
    config: ComponentConfig,
    index: Int,
    error: Throwable
  ): Unit = {
    try {
      // Fire and forget - don't block pipeline
      Future { sendAlert(config, error) }
    } catch {
      case e: Exception =>
        // Log but don't rethrow - hooks shouldn't fail the pipeline
        System.err.println(s"Alert hook failed: ${e.getMessage}")
    }
  }
}
```

## Dry-Run Mode

Validate pipeline configuration without executing components using `dryRun()`:

```scala
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner
import io.github.dwsmith1983.spark.pipeline.config.{DryRunResult, DryRunError}

val result = SimplePipelineRunner.dryRun(config)

result match {
  case DryRunResult.Valid(pipelineName, componentCount, components) =>
    println(s"Pipeline '$pipelineName' is valid with $componentCount components")

  case DryRunResult.Invalid(errors) =>
    errors.foreach {
      case DryRunError.ConfigParseError(msg, _) =>
        println(s"Config error: $msg")
      case DryRunError.ComponentInstantiationError(comp, cause) =>
        println(s"Component '${comp.instanceName}' failed: ${cause.getMessage}")
    }
    sys.exit(1)
}
```

### What Dry-Run Validates

- Configuration parsing (HOCON syntax, required fields)
- Component class existence and accessibility
- Companion object `createFromConfig` method presence
- Component-specific configuration validity

### What Dry-Run Does NOT Do

- Does not call `component.run()`
- Does not read/write data
- Does not require actual Spark cluster resources

### CI/CD Integration

Use dry-run to validate configuration changes before deployment:

```bash
# In your CI pipeline
sbt "runMain com.example.ValidatePipeline /path/to/config.conf"
```

```scala
object ValidatePipeline {
  def main(args: Array[String]): Unit = {
    val configPath = args(0)
    val result = SimplePipelineRunner.dryRunFromFile(configPath)

    if (result.isInvalid) {
      System.err.println("Pipeline validation failed!")
      result.asInstanceOf[DryRunResult.Invalid].errors.foreach { e =>
        System.err.println(s"  - ${e.message}")
      }
      sys.exit(1)
    }

    println("Pipeline configuration is valid")
  }
}
```
