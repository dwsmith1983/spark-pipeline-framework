# spark-pipeline-framework
[![CI](https://github.com/dwsmith1983/spark-pipeline-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/dwsmith1983/spark-pipeline-framework/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.dwsmith1983/spark-pipeline-core_2.13?label=Maven%20Central)](https://central.sonatype.com/namespace/io.github.dwsmith1983)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Scala Versions](https://img.shields.io/badge/scala-2.12%20%7C%202.13-blue)](https://www.scala-lang.org)
[![Spark Versions](https://img.shields.io/badge/spark-3.5.x%20%7C%204.0.x-E25A1C)](https://spark.apache.org)
[![Code style: scalafmt](https://img.shields.io/badge/code%20style-scalafmt-blue)](https://scalameta.org/scalafmt/)

A configuration-driven framework for building Spark pipelines with HOCON config files and PureConfig.

## Features

- **Type-safe configuration** via PureConfig with automatic case class binding
- **SparkSession management** from config files
- **Dynamic component instantiation** via reflection (no compile-time coupling)
- **Lifecycle hooks** for monitoring, metrics, and custom error handling
- **Built-in structured logging** with `LoggingHooks` (JSON format)
- **Built-in metrics** with `MetricsHooks` (Micrometer integration)
- **Built-in audit trails** with `AuditHooks` (JSON Lines, sensitive value filtering)
- **Dry-run validation** for CI/CD config validation without execution
- **Continue-on-error mode** with `failFast=false` for resilient pipelines
- **Cross-compilation** support for Spark 3.x (Scala 2.12, 2.13) and Spark 4.x (Scala 2.13)
- **Clean separation** between framework and user code

## Modules

| Module | Description | Spark Dependency |
|--------|-------------|------------------|
| `spark-pipeline-core` | Traits, config models, instantiation | None |
| `spark-pipeline-runtime` | SparkSessionWrapper, DataFlow trait | Yes (provided) |
| `spark-pipeline-runner` | SimplePipelineRunner entry point | Yes (provided) |

## Quick Start

### 1. Add dependency to your project

Available on [Maven Central](https://central.sonatype.com/namespace/io.github.dwsmith1983):

```scala
// build.sbt - no resolver needed, Maven Central is the default
libraryDependencies += "io.github.dwsmith1983" %% "spark-pipeline-runtime-spark3" % "<version>"
```

### 2. Create a pipeline component

```scala
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow
import pureconfig._
import pureconfig.generic.auto._

object MyComponent extends ConfigurableInstance {
  case class Config(inputTable: String, outputPath: String)

  override def createFromConfig(conf: com.typesafe.config.Config): MyComponent = {
    new MyComponent(ConfigSource.fromConfig(conf).loadOrThrow[Config])
  }
}

class MyComponent(conf: MyComponent.Config) extends DataFlow {
  override def run(): Unit = {
    logInfo(s"Processing ${conf.inputTable}")
    spark.table(conf.inputTable).write.parquet(conf.outputPath)
  }
}
```

### 3. Create a pipeline config file

```hocon
# pipeline.conf
spark {
  app-name = "My Pipeline"
  config {
    "spark.executor.memory" = "4g"
  }
}

pipeline {
  pipeline-name = "My Data Pipeline"
  pipeline-components = [
    {
      instance-type = "com.mycompany.MyComponent"
      instance-name = "MyComponent(prod)"
      instance-config {
        input-table = "raw_data"
        output-path = "/data/processed"
      }
    }
  ]
}
```

### 4. Build and deploy

```bash
# Build your thin JAR
sbt package

# Deploy with spark-submit
spark-submit \
  --class io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner \
  --jars /path/to/my-pipeline.jar \
  /path/to/spark-pipeline-runner-spark3_2.12.jar \
  -Dconfig.file=/path/to/pipeline.conf
```

## Lifecycle Hooks

### Built-in LoggingHooks

Use the built-in `LoggingHooks` for structured logging:

```scala
import io.github.dwsmith1983.spark.pipeline.config.LoggingHooks
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner

// Structured JSON logging (recommended for production)
val hooks = new LoggingHooks()
SimplePipelineRunner.run(config, hooks)

// Human-readable logging for development
val devHooks = LoggingHooks.humanReadable()
SimplePipelineRunner.run(config, devHooks)
```

Output (JSON format):
```json
{"event":"pipeline_start","run_id":"abc-123","pipeline_name":"MyPipeline","component_count":3}
{"event":"component_end","run_id":"abc-123","component_name":"Transform","duration_ms":1523,"status":"success"}
{"event":"pipeline_end","run_id":"abc-123","duration_ms":5000,"status":"success","components_completed":3}
```

### Built-in MetricsHooks

Use the built-in `MetricsHooks` for production metrics via [Micrometer](https://micrometer.io/):

```scala
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.github.dwsmith1983.spark.pipeline.config.MetricsHooks

val registry = new SimpleMeterRegistry() // or PrometheusMeterRegistry, DatadogMeterRegistry, etc.
val hooks = MetricsHooks(registry)
SimplePipelineRunner.run(config, hooks)

// Access metrics
val duration = registry.get("pipeline.duration").timer()
val runs = registry.get("pipeline.component.runs").counter()
```

Metrics include: `pipeline.duration`, `pipeline.runs`, `pipeline.component.duration`, `pipeline.component.runs` - all tagged with `pipeline_name`, `status`, and `component_name`.

### Built-in AuditHooks

Use the built-in `AuditHooks` for persistent execution audit trails with security filtering:

```scala
import io.github.dwsmith1983.spark.pipeline.audit._

// Basic file-based audit trail (JSON Lines format)
val auditHooks = AuditHooks.toFile("/var/log/pipeline-audit.jsonl")
SimplePipelineRunner.run(config, auditHooks)

// With custom run ID for correlation
val auditHooks = AuditHooks.toFile("/var/log/audit.jsonl", runId = "job-12345")

// With custom filtering for sensitive values
val customFilter = ConfigFilter.withAdditionalPatterns(Set("internal_key"))
val auditHooks = new AuditHooks(
  sink = AuditSink.file("/var/log/audit.jsonl"),
  configFilter = customFilter
)
```

AuditHooks captures pipeline/component lifecycle events with rich context:
- System info (hostname, JVM version, Spark version)
- Component configuration (with sensitive values redacted)
- Execution timing and status
- Error details with optional stack traces

Output (JSON Lines format):
```json
{"event_type":"pipeline_start","run_id":"job-123","pipeline_name":"MyPipeline","component_count":3,"timestamp":"..."}
{"event_type":"component_start","component_name":"Transform","component_config":{"password":"***REDACTED***"}}
{"event_type":"component_end","component_name":"Transform","duration_ms":1523,"status":"success"}
{"event_type":"pipeline_end","status":"success","duration_ms":5000,"components_completed":3}
```

### Custom Hooks

For custom behavior, implement the `PipelineHooks` trait:

```scala
import io.github.dwsmith1983.spark.pipeline.config._

val metricsHooks = new PipelineHooks {
  override def afterComponent(config: ComponentConfig, index: Int, total: Int, durationMs: Long): Unit =
    metrics.recordDuration(config.instanceName, durationMs)

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit =
    result match {
      case PipelineResult.Success(duration, count) =>
        metrics.recordSuccess(config.pipelineName, duration)
      case PipelineResult.Failure(error, _, _) =>
        alerting.sendAlert(s"Pipeline failed: ${error.getMessage}")
    }
}
```

Compose multiple hooks:

```scala
val composed = PipelineHooks.compose(new LoggingHooks(), metricsHooks, alertingHooks)
SimplePipelineRunner.run(config, composed)
```

## Running the Demo

The example module includes a complete end-to-end demo showing hooks, metrics, and multi-component pipelines:

```bash
# Build the example JAR
sbt examplespark3/package

# Run with spark-submit
spark-submit \
  --class io.github.dwsmith1983.pipelines.DemoPipeline \
  --master "local[*]" \
  example/target/spark3-jvm-2.13/spark-pipeline-example-spark3_2.13-<version>.jar
```

The demo will:
1. Create sample text data
2. Run a multi-component word analysis pipeline
3. Display real-time progress via logging hooks
4. Generate a metrics report showing component timings

### Sample Output

```
======================================================================
  SPARK PIPELINE FRAMEWORK - END-TO-END DEMO
======================================================================

[SETUP] Creating sample input data...
[SETUP] Input file: /tmp/pipeline-demo/sample-text.txt

[RUNNING] Executing pipeline with composed hooks...

[PIPELINE START] Word Analysis Pipeline (2 components)
  [1/2] Starting: WordCount(all words)
  [1/2] Completed: WordCount(all words) (1842ms)
  [2/2] Starting: WordCount(frequent words only)
  [2/2] Completed: WordCount(frequent words only) (211ms)

[PIPELINE COMPLETE] 2 components executed in 2395ms

======================================================================
  METRICS REPORT
======================================================================
Pipeline: Word Analysis Pipeline
Status: SUCCESS
Total Duration: 2395ms
Components: 2/2 completed

Component Timings:
  WordCount(all words): 1842ms (76.9%)
  WordCount(frequent words only): 211ms (8.8%)

======================================================================
  OUTPUT LOCATIONS
======================================================================
  All words:      /tmp/pipeline-demo/wordcount-output
  Frequent words: /tmp/pipeline-demo/filtered-output

[CLEANUP] Removing temporary files...
[DONE]
```

See `example/src/main/scala/io/github/dwsmith1983/pipelines/` for:
- `WordCount.scala` - Example component
- `DemoMetricsHooks.scala` - Simple in-memory metrics (for learning/demos)
- `DemoPipeline.scala` - End-to-end runnable demo with metrics
- `DemoAuditPipeline.scala` - Audit trail demo with security filtering

**Note:** For production, use the built-in hooks from the core module:
- `MetricsHooks` - Micrometer integration (see [Built-in MetricsHooks](#built-in-metricshooks))
- `AuditHooks` - Persistent audit trails (see [Built-in AuditHooks](#built-in-audithooks))

## Cross-Compilation Matrix

| Artifact | Spark | Scala | Java |
|----------|-------|-------|------|
| `*-spark3_2.12` | 3.5.7 | 2.12.18 | 17+ |
| `*-spark3_2.13` | 3.5.7 | 2.13.12 | 17+ |
| `*-spark4_2.13` | 4.0.1 | 2.13.12 | 17+ |

## Building the Framework

```bash
# Compile all modules
sbt compile

# Run tests
sbt test

# Package JARs
sbt package

# Build runner assembly (optional, for fat JAR)
sbt runner/assembly
```

## Development Setup

### Pre-commit Hooks

Set up pre-commit hooks to catch formatting and style issues before pushing:

**Option 1: Using pre-commit framework (recommended)**
```bash
pip install pre-commit
pre-commit install
```

**Option 2: Manual git hook**
```bash
cp scripts/pre-commit.sh .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

The hooks will automatically run `scalafmtCheckAll`, `scalastyle`, and `scalafixAll --check` before each commit.

### Manual Linting

```bash
# Check formatting (scalafmt)
sbt scalafmtCheckAll

# Auto-fix formatting
sbt scalafmtAll

# Check style rules (scalastyle)
sbt scalastyle

# Check semantic rules (scalafix)
sbt "scalafixAll --check"

# Auto-fix semantic issues
sbt scalafixAll

# Security scan (OWASP Dependency Check)
sbt dependencyCheck
```

## Roadmap

Track progress on [GitHub Milestones](https://github.com/dwsmith1983/spark-pipeline-framework/milestones).

| Version | Focus | Key Features |
|---------|-------|--------------|
| **v1.1.0** | Operational Improvements | Schema contracts ([#49](https://github.com/dwsmith1983/spark-pipeline-framework/issues/49)), Configuration validation ([#54](https://github.com/dwsmith1983/spark-pipeline-framework/issues/54)) |
| **v1.2.0** | Resilience | Checkpointing ([#50](https://github.com/dwsmith1983/spark-pipeline-framework/issues/50)), Retry logic ([#55](https://github.com/dwsmith1983/spark-pipeline-framework/issues/55)), Data quality hooks ([#56](https://github.com/dwsmith1983/spark-pipeline-framework/issues/56)) |
| **v2.0.0** | Parallelism | DAG-based component execution ([#51](https://github.com/dwsmith1983/spark-pipeline-framework/issues/51)) |
| **v2.1.0** | Streaming | Structured Streaming support ([#52](https://github.com/dwsmith1983/spark-pipeline-framework/issues/52)) |
| **v2.2.0** | Cloud Native | Secrets management ([#57](https://github.com/dwsmith1983/spark-pipeline-framework/issues/57)), Spark Connect ([#58](https://github.com/dwsmith1983/spark-pipeline-framework/issues/58)) |

## License

Apache 2.0
