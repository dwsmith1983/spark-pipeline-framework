# Components

Pipeline components are the building blocks of your data pipelines. Each component implements specific data processing logic and is instantiated dynamically from configuration.

## Component Architecture

Components are built using three key traits:

```
ConfigurableInstance (companion object)
    └── createFromConfig(Config): T

DataFlow (class) extends:
    ├── PipelineComponent  → run(): Unit
    ├── SparkSessionWrapper → spark: SparkSession
    └── logger: Logger     → logger.info(), logger.error(), etc.
```

## Creating a Component

### Step 1: Define the Companion Object

The companion object implements `ConfigurableInstance` and handles instantiation:

```scala
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import com.typesafe.config.Config
import pureconfig._
import pureconfig.generic.auto._

object DataTransform extends ConfigurableInstance {
  // Type-safe configuration case class
  case class Config(
    inputPath: String,
    outputPath: String,
    filterColumn: String,
    filterValue: String
  )

  override def createFromConfig(conf: Config): DataTransform = {
    val config = ConfigSource.fromConfig(conf).loadOrThrow[Config]
    new DataTransform(config)
  }
}
```

### Step 2: Implement the Class

The class extends `DataFlow` and implements the `run()` method:

```scala
import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow

class DataTransform(conf: DataTransform.Config) extends DataFlow {

  override def run(): Unit = {
    logger.info(s"Reading from ${conf.inputPath}")

    val data = spark.read.parquet(conf.inputPath)
      .filter(col(conf.filterColumn) === conf.filterValue)
      .select("id", "name", "timestamp")

    logger.info(s"Writing to ${conf.outputPath}")
    data.write.mode("overwrite").parquet(conf.outputPath)

    logger.info(s"Completed with ${data.count()} records")
  }
}
```

### Step 3: Configure in HOCON

```json
pipeline {
  pipeline-components = [
    {
      instance-type = "com.example.DataTransform"
      instance-name = "FilterActiveUsers"
      instance-config {
        input-path = "/data/users"
        output-path = "/data/active_users"
        filter-column = "status"
        filter-value = "active"
      }
    }
  ]
}
```

> **See Also**: For complete, runnable examples, refer to [BatchPipelineExample](https://github.com/dwsmith1983/spark-pipeline-framework/blob/main/example/src/main/scala/io/github/dwsmith1983/pipelines/BatchPipelineExample.scala) which demonstrates ETL, aggregation, and enrichment patterns.

## SparkSession Access

The `DataFlow` trait provides access to a configured SparkSession:

```scala
class MyComponent(conf: MyComponent.Config) extends DataFlow {
  override def run(): Unit = {
    // SparkSession is available via 'spark'
    val df = spark.read.parquet(conf.inputPath)

    // SparkContext is available via spark.sparkContext
    val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3))

    // SQL context
    spark.sql("SELECT * FROM my_table")
  }
}
```

## Logging

The `DataFlow` trait provides a Log4j2 logger instance:

```scala
class MyComponent(conf: MyComponent.Config) extends DataFlow {
  override def run(): Unit = {
    logger.info("Starting processing")
    logger.debug(s"Configuration: $conf")

    try {
      // processing...
    } catch {
      case e: Exception =>
        logger.error("Processing failed", e)
        throw e
    }

    logger.info("Processing complete")
  }
}
```

Available logging methods:
- `logger.trace(msg)`, `logger.trace(msg, throwable)`
- `logger.debug(msg)`, `logger.debug(msg, throwable)`
- `logger.info(msg)`, `logger.info(msg, throwable)`
- `logger.warn(msg)`, `logger.warn(msg, throwable)`
- `logger.error(msg)`, `logger.error(msg, throwable)`

## Configuration Patterns

### Optional Fields with Defaults

```scala
case class Config(
  inputPath: String,                    // Required
  outputPath: String,                   // Required
  partitionBy: List[String] = List.empty,  // Optional with default
  coalesce: Option[Int] = None          // Optional, may be absent
)
```

### Nested Configuration

```scala
case class FilterConfig(column: String, value: String)
case class Config(
  inputPath: String,
  outputPath: String,
  filters: List[FilterConfig] = List.empty
)
```

HOCON:
```json
instance-config {
  input-path = "/data/input"
  output-path = "/data/output"
  filters = [
    { column = "status", value = "active" },
    { column = "region", value = "US" }
  ]
}
```

### Sealed Trait Configuration

```scala
sealed trait OutputFormat
case object Parquet extends OutputFormat
case class Csv(delimiter: String = ",") extends OutputFormat

case class Config(
  inputPath: String,
  outputPath: String,
  format: OutputFormat = Parquet
)
```

## Error Handling

Throw exceptions to signal component failure:

```scala
class MyComponent(conf: MyComponent.Config) extends DataFlow {
  override def run(): Unit = {
    val df = spark.read.parquet(conf.inputPath)

    if (df.isEmpty) {
      throw new IllegalStateException(s"No data found at ${conf.inputPath}")
    }

    // Continue processing...
  }
}
```

The framework captures exceptions and reports them via [lifecycle hooks](./hooks).

## Testing Components

```scala
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.SparkSession

class DataTransformSpec extends AnyFlatSpec {
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()

  "DataTransform" should "filter data correctly" in {
    val tempDir = Files.createTempDirectory("test")
    val inputPath = tempDir.resolve("input")
    val outputPath = tempDir.resolve("output")

    // Create test data
    import spark.implicits._
    val testData = Seq(
      ("1", "Alice", "active"),
      ("2", "Bob", "inactive"),
      ("3", "Charlie", "active")
    ).toDF("id", "name", "status")
    testData.write.parquet(inputPath.toString)

    val config = DataTransform.Config(
      inputPath = inputPath.toString,
      outputPath = outputPath.toString,
      filterColumn = "status",
      filterValue = "active"
    )

    val component = new DataTransform(config)
    component.run()

    val result = spark.read.parquet(outputPath.toString)
    assert(result.count() == 2)
  }
}
```

## Next Steps

- [Lifecycle Hooks](./hooks) - Monitor component execution and handle errors
