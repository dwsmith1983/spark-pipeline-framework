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
    └── Logging            → logInfo(), logError(), etc.
```

## Creating a Component

### Step 1: Define the Companion Object

The companion object implements `ConfigurableInstance` and handles instantiation:

```scala
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import com.typesafe.config.Config
import pureconfig._
import pureconfig.generic.auto._

object WordCount extends ConfigurableInstance {
  // Type-safe configuration case class
  case class Config(
    inputPath: String,
    outputPath: String,
    minCount: Int = 1
  )

  override def createFromConfig(conf: Config): WordCount = {
    val config = ConfigSource.fromConfig(conf).loadOrThrow[Config]
    new WordCount(config)
  }
}
```

### Step 2: Implement the Class

The class extends `DataFlow` and implements the `run()` method:

```scala
import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow

class WordCount(conf: WordCount.Config) extends DataFlow {

  override def run(): Unit = {
    logInfo(s"Reading from ${conf.inputPath}")

    val words = spark.read.textFile(conf.inputPath)
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .groupBy("value")
      .count()
      .filter($"count" >= conf.minCount)

    logInfo(s"Writing to ${conf.outputPath}")
    words.write.mode("overwrite").parquet(conf.outputPath)

    logInfo(s"Completed with ${words.count()} words")
  }
}
```

### Step 3: Configure in HOCON

```json
pipeline {
  pipeline-components = [
    {
      instance-type = "com.example.WordCount"
      instance-name = "WordCount(articles)"
      instance-config {
        input-path = "/data/articles/*.txt"
        output-path = "/data/wordcounts"
        min-count = 5
      }
    }
  ]
}
```

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

The `DataFlow` trait includes Log4j2 logging:

```scala
class MyComponent(conf: MyComponent.Config) extends DataFlow {
  override def run(): Unit = {
    logInfo("Starting processing")
    logDebug(s"Configuration: $conf")

    try {
      // processing...
    } catch {
      case e: Exception =>
        logError("Processing failed", e)
        throw e
    }

    logInfo("Processing complete")
  }
}
```

Available logging methods:
- `logTrace(msg)`, `logTrace(msg, throwable)`
- `logDebug(msg)`, `logDebug(msg, throwable)`
- `logInfo(msg)`, `logInfo(msg, throwable)`
- `logWarning(msg)`, `logWarning(msg, throwable)`
- `logError(msg)`, `logError(msg, throwable)`

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

class WordCountSpec extends AnyFlatSpec {
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()

  "WordCount" should "count words correctly" in {
    val tempDir = Files.createTempDirectory("test")
    val inputPath = tempDir.resolve("input.txt")
    val outputPath = tempDir.resolve("output")

    Files.write(inputPath, "hello world hello".getBytes)

    val config = WordCount.Config(
      inputPath = inputPath.toString,
      outputPath = outputPath.toString,
      minCount = 1
    )

    val component = new WordCount(config)
    component.run()

    val result = spark.read.parquet(outputPath.toString)
    assert(result.count() == 2)
  }
}
```

## Next Steps

- [Lifecycle Hooks](./hooks) - Monitor component execution and handle errors
