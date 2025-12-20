# Lifecycle Hooks

Lifecycle hooks allow you to monitor pipeline execution, collect metrics, handle errors, and integrate with external systems without modifying your components.

## PipelineHooks Trait

The `PipelineHooks` trait provides callbacks for key pipeline events:

```scala
import io.github.dwsmith1983.spark.pipeline.config._

trait PipelineHooks {
  def beforePipeline(config: PipelineConfig): Unit = {}
  def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {}
  def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit = {}
  def afterComponent(config: ComponentConfig, index: Int, total: Int, durationMs: Long): Unit = {}
  def onComponentFailure(config: ComponentConfig, index: Int, total: Int, error: Throwable): Unit = {}
}
```

## Pipeline Result

The `PipelineResult` sealed trait represents pipeline completion status:

```scala
sealed trait PipelineResult

object PipelineResult {
  case class Success(
    totalDurationMs: Long,
    componentCount: Int
  ) extends PipelineResult

  case class Failure(
    error: Throwable,
    failedComponentIndex: Int,
    failedComponentName: String
  ) extends PipelineResult
}
```

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

## Metrics Collection Example

```scala
import scala.collection.mutable

class MetricsHooks extends PipelineHooks {
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
    total: Int,
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
    total: Int,
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
