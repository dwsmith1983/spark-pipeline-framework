# Streaming Pipelines

Build Spark Structured Streaming applications with the same configuration-driven approach as batch pipelines. Connect sources to sinks with optional transformations and automatic watermarking.

## Quick Start

Create a streaming pipeline by extending `StreamingPipeline`:

```scala
import io.github.dwsmith1983.spark.pipeline.streaming._
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.OutputMode
import pureconfig._
import pureconfig.generic.auto._

object MyStreamingPipeline extends ConfigurableInstance {
  case class Config(
    kafkaServers: String,
    inputTopic: String,
    outputPath: String,
    checkpointPath: String
  )

  override def createFromConfig(conf: com.typesafe.config.Config): MyStreamingPipeline =
    new MyStreamingPipeline(ConfigSource.fromConfig(conf).loadOrThrow[Config])
}

class MyStreamingPipeline(conf: MyStreamingPipeline.Config) extends StreamingPipeline {

  override def source: StreamingSource = new StreamingSource {
    override def readStream(): DataFrame =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", conf.kafkaServers)
        .option("subscribe", conf.inputTopic)
        .load()
  }

  override def sink: StreamingSink = new StreamingSink {
    override def writeStream(df: DataFrame) =
      df.writeStream.format("parquet").option("path", conf.outputPath)

    override def outputMode = OutputMode.Append()
    override def checkpointLocation = conf.checkpointPath
  }
}
```

## Core Components

### StreamingPipeline

The main orchestrator that connects sources to sinks:

| Method | Description |
|--------|-------------|
| `source` | Returns the `StreamingSource` to read from |
| `sink` | Returns the `StreamingSink` to write to |
| `transform(df)` | Optional transformation between source and sink |
| `trigger` | Trigger configuration (defaults to ProcessingTime 0s) |
| `startStream()` | Starts query, returns `StreamingQuery` handle |
| `run()` | Starts query and blocks until termination |

### StreamingSource

Base trait for streaming data sources:

```scala
trait StreamingSource extends DataFlow {
  def readStream(): DataFrame
  def watermarkColumn: Option[String] = None
  def watermarkDelay: Option[String] = None
}
```

### StreamingSink

Base trait for streaming data sinks:

```scala
trait StreamingSink extends DataFlow {
  def writeStream(df: DataFrame): DataStreamWriter[Row]
  def outputMode: OutputMode
  def checkpointLocation: String
  def queryName: Option[String] = None
}
```

## Trigger Configuration

Control how often micro-batches execute:

```scala
import io.github.dwsmith1983.spark.pipeline.streaming.TriggerConfig

// Process every 10 seconds (most common)
override def trigger = TriggerConfig.ProcessingTime("10 seconds")

// Run once for testing or batch-like processing
override def trigger = TriggerConfig.Once

// Process all available data then stop (Spark 3.3+)
override def trigger = TriggerConfig.AvailableNow

// Low-latency continuous processing (experimental)
override def trigger = TriggerConfig.Continuous("1 second")
```

| Trigger Type | Use Case |
|--------------|----------|
| `ProcessingTime` | Regular streaming workloads |
| `Once` | Testing, one-time migrations |
| `AvailableNow` | Catch up on backlog then stop |
| `Continuous` | Sub-millisecond latency (limited sink support) |

## Watermarking

Enable event-time processing with automatic watermark application:

```scala
class MySource extends StreamingSource {
  override def readStream(): DataFrame = ???

  // Define watermark column and delay
  override def watermarkColumn: Option[String] = Some("event_time")
  override def watermarkDelay: Option[String] = Some("10 minutes")
}
```

The `StreamingPipeline` automatically applies watermarking when both `watermarkColumn` and `watermarkDelay` are defined.

## Output Modes

Spark Structured Streaming supports three output modes:

| Mode | Description | Use When |
|------|-------------|----------|
| `Append` | Only new rows written | Most streaming workloads |
| `Complete` | Entire result table written | Aggregations |
| `Update` | Only changed rows written | Stateful operations |

```scala
import org.apache.spark.sql.streaming.OutputMode

override def outputMode = OutputMode.Append()   // Default for most sinks
override def outputMode = OutputMode.Complete() // For aggregations
override def outputMode = OutputMode.Update()   // For stateful ops
```

## Checkpointing

Every streaming sink requires a checkpoint location for fault tolerance:

```scala
override def checkpointLocation = "s3://bucket/checkpoints/my-pipeline"
```

**Best practices:**
- Use durable storage (HDFS, S3, GCS) for production
- Each streaming query needs a unique checkpoint path
- Never share checkpoint locations between queries

## Lifecycle Hooks

Monitor streaming queries with `StreamingHooks`:

```scala
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingHooks

val myHooks = new StreamingHooks {
  override def onQueryStart(queryName: String, queryId: String): Unit =
    println(s"Query started: $queryName ($queryId)")

  override def onBatchProgress(
    queryName: String,
    batchId: Long,
    numInputRows: Long,
    durationMs: Long
  ): Unit =
    println(s"Batch $batchId: $numInputRows rows in ${durationMs}ms")

  override def onQueryTerminated(
    queryName: String,
    queryId: String,
    exception: Option[Throwable]
  ): Unit =
    exception.foreach(e => alerting.send(s"Query $queryName failed: ${e.getMessage}"))
}

// Compose multiple hooks
val compositeHooks = StreamingHooks.compose(loggingHooks, metricsHooks, alertingHooks)
```

## Complete Example

A Kafka-to-Delta streaming pipeline:

```scala
import io.github.dwsmith1983.spark.pipeline.streaming._
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.OutputMode
import pureconfig._
import pureconfig.generic.auto._

object KafkaToDeltaPipeline extends ConfigurableInstance {
  case class Config(
    kafkaServers: String,
    topic: String,
    deltaTablePath: String,
    checkpointPath: String,
    triggerInterval: String = "30 seconds"
  )

  override def createFromConfig(conf: com.typesafe.config.Config): KafkaToDeltaPipeline =
    new KafkaToDeltaPipeline(ConfigSource.fromConfig(conf).loadOrThrow[Config])
}

class KafkaToDeltaPipeline(conf: KafkaToDeltaPipeline.Config) extends StreamingPipeline {

  override def source: StreamingSource = new StreamingSource {
    override def readStream(): DataFrame =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", conf.kafkaServers)
        .option("subscribe", conf.topic)
        .option("startingOffsets", "latest")
        .load()

    override def watermarkColumn = Some("timestamp")
    override def watermarkDelay = Some("5 minutes")
  }

  override def sink: StreamingSink = new StreamingSink {
    override def writeStream(df: DataFrame) =
      df.writeStream
        .format("delta")
        .option("path", conf.deltaTablePath)
        .option("mergeSchema", "true")

    override def outputMode = OutputMode.Append()
    override def checkpointLocation = conf.checkpointPath
    override def queryName = Some("kafka-to-delta")
  }

  override def transform(df: DataFrame): DataFrame = {
    import spark.implicits._
    df.selectExpr(
      "CAST(key AS STRING) as key",
      "CAST(value AS STRING) as value",
      "timestamp",
      "partition",
      "offset"
    ).filter($"value".isNotNull)
  }

  override def trigger = TriggerConfig.ProcessingTime(conf.triggerInterval)
}
```

Configuration:

```hocon
pipeline {
  pipeline-name = "Kafka to Delta Pipeline"
  pipeline-components = [
    {
      instance-type = "com.example.KafkaToDeltaPipeline"
      instance-name = "kafka-to-delta"
      instance-config {
        kafka-servers = "kafka:9092"
        topic = "events"
        delta-table-path = "/data/delta/events"
        checkpoint-path = "/checkpoints/kafka-to-delta"
        trigger-interval = "30 seconds"
      }
    }
  ]
}
```

## Testing

Use `TriggerConfig.Once` for testing:

```scala
class StreamingPipelineSpec extends AnyFlatSpec {
  "MyStreamingPipeline" should "process data correctly" in {
    val testPipeline = new MyStreamingPipeline(testConfig) {
      override def trigger = TriggerConfig.Once
    }

    val query = testPipeline.startStream()
    query.awaitTermination()

    // Assert on output
  }
}
```

For testing sources and sinks independently, use Spark's built-in `rate` source:

```scala
// Test sink with rate source
val testSource = new StreamingSource {
  override def readStream(): DataFrame =
    spark.readStream.format("rate").option("rowsPerSecond", 10).load()
}
```
