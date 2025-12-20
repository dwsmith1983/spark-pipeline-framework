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
| `master` | String | No | Spark master URL (prefer spark-submit) |
| `config` | Map | No | Key-value pairs for SparkConf |

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

## Next Steps

- [Components](./components) - Learn how to build pipeline components
- [Lifecycle Hooks](./hooks) - Add monitoring and error handling
