# Checkpointing and Resume

Spark Pipeline Framework supports checkpointing for fault tolerance, enabling pipelines to resume from the last successful component after a failure instead of restarting from the beginning.

## Overview

Checkpointing works by:
1. Saving component completion state after each successful step
2. Persisting checkpoint data to a configurable storage location
3. Allowing failed pipelines to resume from the last checkpoint

This is particularly useful for:
- Long-running pipelines with expensive computations
- Pipelines that may fail due to transient errors
- Recovery from infrastructure issues

## Quick Start

Enable checkpointing in your pipeline configuration:

```json
pipeline {
  pipeline-name = "My ETL Pipeline"

  checkpoint {
    enabled = true
    location = "/tmp/spark-checkpoints"
  }

  pipeline-components = [
    // ...
  ]
}
```

Run with checkpoint support:

```scala
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner

// Runs with checkpointing enabled per config
SimplePipelineRunner.runWithCheckpointing("/path/to/pipeline.conf")
```

## Configuration Options

The `checkpoint` block supports the following options:

```json
pipeline {
  checkpoint {
    enabled = true                           // Enable checkpointing
    location = "/data/checkpoints"           // Storage location
    auto-resume = false                      // Auto-resume on startup
    cleanup-on-success = true                // Delete checkpoint on success
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | Boolean | `false` | Whether checkpointing is active |
| `location` | String | System temp | Directory for checkpoint storage |
| `auto-resume` | Boolean | `false` | Automatically resume from last failed checkpoint |
| `cleanup-on-success` | Boolean | `true` | Delete checkpoint files after successful run |

### Storage Locations

Checkpoint location can be:
- **Local filesystem**: `/tmp/checkpoints` or `file:///tmp/checkpoints`
- **HDFS**: `hdfs:///spark-checkpoints`
- **S3**: `s3a://my-bucket/checkpoints`

## Resuming Failed Pipelines

### Manual Resume

When a pipeline fails, you can resume it by specifying the run ID:

```scala
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner

// Resume from a specific failed run
SimplePipelineRunner.resume(
  configPath = "/path/to/pipeline.conf",
  runId = "abc123-def456"
)
```

### Auto-Resume Mode

With `auto-resume = true`, the pipeline automatically resumes from the last failed checkpoint:

```json
pipeline {
  checkpoint {
    enabled = true
    location = "/data/checkpoints"
    auto-resume = true
  }
}
```

```scala
// Automatically resumes if a failed checkpoint exists
SimplePipelineRunner.runWithCheckpointing("/path/to/pipeline.conf")
```

### Listing Resumable Checkpoints

Find all failed checkpoints that can be resumed:

```scala
import io.github.dwsmith1983.spark.pipeline.checkpoint._
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner

val store = FileSystemCheckpointStore("/data/checkpoints")
val checkpoints = SimplePipelineRunner.listResumableCheckpoints("My ETL Pipeline", store)

checkpoints.foreach { cp =>
  println(s"Run: ${cp.runId}")
  println(s"  Failed at: component ${cp.lastCheckpointedIndex + 2}/${cp.totalComponents}")
  println(s"  Started: ${cp.startedAt}")
}
```

## Programmatic Usage

### Using CheckpointHooks Directly

You can use checkpoint hooks independently for more control:

```scala
import io.github.dwsmith1983.spark.pipeline.checkpoint._
import io.github.dwsmith1983.spark.pipeline.config.PipelineHooks
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner

// Create checkpoint store and hooks
val store = FileSystemCheckpointStore("/data/checkpoints")
val checkpointHooks = CheckpointHooks(store)

// Combine with other hooks
val loggingHooks = LoggingHooks.structured()
val combined = PipelineHooks.compose(checkpointHooks, loggingHooks)

// Run with combined hooks
SimplePipelineRunner.run(config, combined)
```

### Custom Checkpoint Store

Implement `CheckpointStore` for custom storage backends:

```scala
import io.github.dwsmith1983.spark.pipeline.checkpoint._

class RedisCheckpointStore(host: String) extends CheckpointStore {
  override def save(state: CheckpointState): Unit = {
    // Save to Redis
  }

  override def loadLatest(pipelineName: String): Option[CheckpointState] = {
    // Load from Redis
  }

  // ... implement other methods
}
```

## Checkpoint State

Each checkpoint captures:

| Field | Description |
|-------|-------------|
| `runId` | Unique identifier for the pipeline run |
| `pipelineName` | Name from pipeline configuration |
| `startedAt` | When the pipeline started |
| `completedComponents` | List of successfully completed components |
| `lastCheckpointedIndex` | Index of last successful component |
| `totalComponents` | Total number of components |
| `status` | Running, Failed, or Completed |
| `resumedFrom` | Original runId if this is a resumed run |

## Best Practices

### 1. Use Persistent Storage for Production

```json
checkpoint {
  enabled = true
  location = "s3a://my-bucket/spark-checkpoints"
}
```

### 2. Set Appropriate Cleanup Policy

- Use `cleanup-on-success = true` for most cases to avoid checkpoint accumulation
- Set `cleanup-on-success = false` if you need audit trails

### 3. Handle Configuration Changes

Checkpointing validates that the pipeline configuration matches before resuming. If you change the number of components, you cannot resume from an old checkpoint.

### 4. Combine with Other Hooks

```scala
val hooks = PipelineHooks.compose(
  CheckpointHooks(store),      // Checkpointing
  AuditHooks.toFile(auditPath), // Audit trail
  MetricsHooks(registry)        // Metrics
)
```

## Limitations

- **Sequential execution only**: Checkpointing works with the current sequential component model
- **No sub-component checkpoints**: Checkpoints are at component granularity
- **Config must match**: Cannot resume if pipeline configuration changed
- **Metadata only**: Checkpoints don't store intermediate DataFrames

## Example: Full Pipeline with Checkpointing

```json
spark {
  app-name = "Daily ETL"
  config {
    "spark.sql.shuffle.partitions" = "200"
  }
}

pipeline {
  pipeline-name = "Daily ETL Pipeline"
  fail-fast = true

  checkpoint {
    enabled = true
    location = "s3a://data-lake/checkpoints"
    auto-resume = true
    cleanup-on-success = true
  }

  pipeline-components = [
    {
      instance-type = "com.example.Extract"
      instance-name = "Extract(orders)"
      instance-config { ... }
    },
    {
      instance-type = "com.example.Transform"
      instance-name = "Transform(clean)"
      instance-config { ... }
    },
    {
      instance-type = "com.example.Load"
      instance-name = "Load(warehouse)"
      instance-config { ... }
    }
  ]
}
```

```scala
// Run with auto-resume - if Transform failed yesterday,
// today's run will skip Extract and continue from Transform
SimplePipelineRunner.runWithCheckpointing("/conf/daily-etl.conf")
```

## Next Steps

- [Configuration](./configuration) - Learn about HOCON configuration
- [Lifecycle Hooks](./hooks) - Add monitoring and error handling
- [Deployment](./deployment) - Deploy pipelines to production
