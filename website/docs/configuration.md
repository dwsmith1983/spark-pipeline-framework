# Configuration

Spark Pipeline Framework uses [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md) (Human-Optimized Config Object Notation) for configuration. HOCON is a superset of JSON that supports comments, substitutions, and includes.

## Configuration Structure

A complete pipeline configuration has two main sections:

```json
spark {
  // SparkSession configuration
}

pipeline {
  // Pipeline and component configuration
}
```

## Spark Configuration

The `spark` block configures the SparkSession:

```json
spark {
  app-name = "My Pipeline"
  master = "local[*]"     // Optional, usually set via spark-submit

  config {
    // Spark configuration properties
    "spark.executor.memory" = "4g"
    "spark.executor.cores" = "2"
    "spark.sql.shuffle.partitions" = "200"
    "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"
  }
}
```

### SparkConfig Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `app-name` | String | Yes | Application name shown in Spark UI |
| `master` | String | No | Spark master URL (prefer spark-submit). Ignored if `connect-string` is set. |
| `config` | Map | No | Key-value pairs for SparkConf |
| `connect-string` | String | No | Spark Connect connection string (sc://host:port). When set, creates a remote Spark Connect session instead of local. Requires Spark 3.4+. |

### Spark Connect Support (Spark 3.4+)

Starting from Spark 3.4, the framework supports [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) for remote Spark connectivity. Spark Connect allows you to:

- Connect to remote Spark clusters without embedding the Spark driver
- Deploy thin clients with minimal Spark dependencies
- Achieve better resource isolation between client and cluster
- Use managed services like Databricks Connect

#### Local Spark Session (Default)

Traditional local or cluster deployment:

```json
spark {
  app-name = "My Pipeline"
  master = "local[*]"  // or yarn, spark://host:port

  config {
    "spark.executor.memory" = "4g"
    "spark.executor.cores" = "2"
  }
}
```

#### Spark Connect Mode

Connect to a remote Spark Connect server:

```json
spark {
  app-name = "My Pipeline"
  connect-string = "sc://spark-server:15002"

  config {
    "spark.executor.memory" = "4g"
    "spark.executor.cores" = "2"
  }
}
```

#### Databricks Connect

Connect to Databricks using Spark Connect:

```json
spark {
  app-name = "My Pipeline"
  connect-string = "sc://your-workspace.cloud.databricks.com"

  config {
    "spark.databricks.token" = ${?DATABRICKS_TOKEN}
    "spark.databricks.cluster.id" = "your-cluster-id"
  }
}
```

Run with:
```bash
export DATABRICKS_TOKEN=dapi123456789...
spark-submit --class io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner \
  --master local[1] \
  your-pipeline.jar
```

#### Spark Connect Benefits

**Thin Client Deployment:**
- Client applications don't need full Spark driver dependencies
- Smaller application JARs and faster deployment
- Better suited for containerized environments

**Resource Isolation:**
- Spark driver runs on the server, not the client
- Client failures don't affect the Spark cluster
- Multiple clients can share a single Spark Connect server

**Cloud-Native:**
- Works seamlessly with managed Spark services
- Supports Databricks, AWS EMR Serverless, and other cloud platforms
- Simplifies authentication and connection management

#### Version Requirements

- Spark Connect requires **Spark 3.4 or later**
- The framework automatically detects Spark version at runtime
- If you use Spark Connect with an earlier version, you'll get a clear error message:
  ```
  UnsupportedOperationException: Spark Connect requires Spark 3.4 or later.
  ```

## Pipeline Configuration

The `pipeline` block defines the pipeline and its components:

```json
pipeline {
  pipeline-name = "ETL Pipeline"

  pipeline-components = [
    {
      instance-type = "com.example.ExtractComponent"
      instance-name = "Extract(source=orders)"
      instance-config {
        source-table = "raw_orders"
        target-path = "/data/staging/orders"
      }
    },
    {
      instance-type = "com.example.TransformComponent"
      instance-name = "Transform(clean)"
      instance-config {
        input-path = "/data/staging/orders"
        output-path = "/data/processed/orders"
        dedup-columns = ["order_id"]
      }
    }
  ]
}
```

### PipelineConfig Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `pipeline-name` | String | Yes | Human-readable pipeline name |
| `pipeline-components` | List | Yes | Ordered list of components to execute |
| `fail-fast` | Boolean | No | If `true` (default), stop on first component failure. If `false`, continue executing remaining components and report all failures. |

### Error Handling Modes

By default, the pipeline stops immediately when a component fails (`fail-fast = true`). You can change this behavior to continue execution despite failures:

```json
pipeline {
  pipeline-name = "Resilient Pipeline"
  fail-fast = false  // Continue after component failures

  pipeline-components = [
    // Components will all be attempted even if some fail
  ]
}
```

When `fail-fast = false`:
- All components are attempted regardless of earlier failures
- `onComponentFailure` hook is called for each failed component
- Pipeline returns `PartialSuccess` result with details of all failures
- Useful for batch processing where some failures are acceptable

### ComponentConfig Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `instance-type` | String | Yes | Fully qualified class name of companion object |
| `instance-name` | String | Yes | Human-readable name for logging |
| `instance-config` | Object | Yes | Component-specific configuration |

## Configuration Loading

Configuration is loaded using [Typesafe Config](https://github.com/lightbend/config):

```bash
# From file
spark-submit ... -Dconfig.file=/path/to/pipeline.conf

# From classpath resource
spark-submit ... -Dconfig.resource=pipelines/my-pipeline.conf

# From URL
spark-submit ... -Dconfig.url=http://config-server/pipeline.conf
```

## Environment Variable Substitution

HOCON supports environment variable substitution:

```json
spark {
  app-name = "Pipeline-"${ENV}
}

pipeline {
  pipeline-components = [
    {
      instance-type = "com.example.MyComponent"
      instance-name = "MyComponent"
      instance-config {
        input-path = ${INPUT_PATH}
        output-path = ${OUTPUT_PATH}
        batch-date = ${?BATCH_DATE}  // Optional, won't fail if missing
      }
    }
  ]
}
```

Run with:
```bash
ENV=prod INPUT_PATH=/data/input OUTPUT_PATH=/data/output spark-submit ...
```

## Configuration Includes

Split configuration across files:

```json
# base.conf
spark {
  config {
    "spark.sql.shuffle.partitions" = "200"
  }
}

# pipeline.conf
include "base.conf"

spark {
  app-name = "My Pipeline"
}

pipeline {
  // ...
}
```

## Type-Safe Configuration with PureConfig

Components use PureConfig for type-safe configuration parsing:

```scala
object MyComponent extends ConfigurableInstance {
  case class Config(
    inputPath: String,
    outputPath: String,
    partitionColumns: List[String] = List.empty,
    overwrite: Boolean = true
  )

  override def createFromConfig(conf: com.typesafe.config.Config): MyComponent = {
    import pureconfig._
    import pureconfig.generic.auto._
    new MyComponent(ConfigSource.fromConfig(conf).loadOrThrow[Config])
  }
}
```

HOCON uses kebab-case (`partition-columns`), which PureConfig automatically maps to camelCase (`partitionColumns`).

## Configuration Errors

When configuration parsing fails, the framework throws a `ConfigurationException` with a detailed error message:

```scala
import io.github.dwsmith1983.spark.pipeline.config.ConfigurationException

try {
  SimplePipelineRunner.run(config)
} catch {
  case e: ConfigurationException =>
    // e.getMessage contains formatted error details
    // e.getCause contains the underlying parsing error
    println(s"Configuration error: ${e.getMessage}")
}
```

Common configuration errors:
- **Missing required fields**: `Key not found: 'input-path'`
- **Type mismatches**: `Expected type STRING, found NUMBER at 'output-path'`
- **Invalid HOCON syntax**: Unbalanced braces, missing quotes, etc.

The error message includes the field path and expected type, making it easy to locate and fix configuration issues.

## Next Steps

- [Components](./components) - Learn how to build pipeline components
- [Lifecycle Hooks](./hooks) - Add monitoring and error handling
