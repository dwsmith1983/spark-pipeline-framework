# spark-pipeline-framework
[![CI](https://github.com/dwsmith1983/spark-pipeline-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/dwsmith1983/spark-pipeline-framework/actions/workflows/ci.yml)
[![GitHub Packages](https://img.shields.io/badge/GitHub%20Packages-published-brightgreen?logo=github)](https://github.com/dwsmith1983?tab=packages&repo_name=spark-pipeline-framework)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Scala Versions](https://img.shields.io/badge/scala-2.12%20%7C%202.13-blue)](https://www.scala-lang.org)
[![Spark Versions](https://img.shields.io/badge/spark-3.5.x%20%7C%204.0.x-E25A1C)](https://spark.apache.org)
[![Code style: scalafmt](https://img.shields.io/badge/code%20style-scalafmt-blue)](https://scalameta.org/scalafmt/)

A configuration-driven framework for building Spark pipelines with HOCON config files and PureConfig.

## Features

- **Type-safe configuration** via PureConfig with automatic case class binding
- **SparkSession management** from config files
- **Dynamic component instantiation** via reflection (no compile-time coupling)
- **Cross-compilation** support for Spark 3.x (Scala 2.12) and Spark 4.x (Scala 2.13)
- **Clean separation** between framework and user code

## Modules

| Module | Description | Spark Dependency |
|--------|-------------|------------------|
| `spark-pipeline-core` | Traits, config models, instantiation | None |
| `spark-pipeline-runtime` | SparkSessionWrapper, DataFlow trait | Yes (provided) |
| `spark-pipeline-runner` | SimplePipelineRunner entry point | Yes (provided) |

## Quick Start

### 1. Add dependency to your project

```scala
// build.sbt
libraryDependencies += "io.github.dwsmith1983" %% "spark-pipeline-runtime-spark3" % "0.1.0"
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

## Cross-Compilation Matrix

| Artifact | Spark | Scala | Java |
|----------|-------|-------|------|
| `*-spark3_2.12` | 3.5.7 | 2.12.18 | 17+ |
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

## License

Apache 2.0
