# spark-pipeline-framework

[![CI](https://github.com/dwsmith1983/spark-pipeline-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/dwsmith1983/spark-pipeline-framework/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.dwsmith1983/spark-pipeline-core_2.13?label=Maven%20Central)](https://central.sonatype.com/namespace/io.github.dwsmith1983)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Scala Versions](https://img.shields.io/badge/scala-2.12%20%7C%202.13-blue)](https://www.scala-lang.org)
[![Spark Versions](https://img.shields.io/badge/spark-3.5.x%20%7C%204.0.x-E25A1C)](https://spark.apache.org)

A configuration-driven framework for building Spark pipelines with HOCON config files and PureConfig.

## Features

- **Type-safe configuration** via PureConfig with automatic case class binding
- **Dynamic component instantiation** via reflection (no compile-time coupling)
- **Lifecycle hooks** for monitoring, metrics, and custom error handling
- **Built-in hooks** for structured logging, Micrometer metrics, and audit trails
- **Configuration validation** for CI/CD pre-flight checks without Spark
- **Secrets management** with pluggable providers (env, AWS, Vault)
- **Streaming support** for Spark Structured Streaming pipelines
- **Cross-compilation** for Spark 3.x/4.x and Scala 2.12/2.13

📚 **[Full Documentation](https://dwsmith1983.github.io/spark-pipeline-framework/)**

## Modules

| Module | Description |
|--------|-------------|
| `spark-pipeline-core` | Traits, config models, instantiation (no Spark dependency) |
| `spark-pipeline-runtime` | SparkSessionWrapper, DataFlow trait |
| `spark-pipeline-runner` | SimplePipelineRunner entry point |

## Quick Start

### 1. Add dependency

```scala
// build.sbt
libraryDependencies += "io.github.dwsmith1983" %% "spark-pipeline-runtime-spark3" % "<version>"
```

### 2. Create a component

```scala
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow
import pureconfig._
import pureconfig.generic.auto._

object MyComponent extends ConfigurableInstance {
  case class Config(inputTable: String, outputPath: String)

  override def createFromConfig(conf: com.typesafe.config.Config): MyComponent =
    new MyComponent(ConfigSource.fromConfig(conf).loadOrThrow[Config])
}

class MyComponent(conf: MyComponent.Config) extends DataFlow {
  override def run(): Unit = {
    spark.table(conf.inputTable).write.parquet(conf.outputPath)
  }
}
```

### 3. Create config file

```hocon
# pipeline.conf
spark {
  app-name = "My Pipeline"
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

### 4. Run

```bash
spark-submit \
  --class io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner \
  --jars /path/to/my-pipeline.jar \
  /path/to/spark-pipeline-runner-spark3_2.12.jar \
  -Dconfig.file=/path/to/pipeline.conf
```

## Documentation

- [Getting Started](https://dwsmith1983.github.io/spark-pipeline-framework/docs/getting-started) - Quick start guide
- [Configuration](https://dwsmith1983.github.io/spark-pipeline-framework/docs/configuration) - HOCON configuration reference
- [Config Validation](https://dwsmith1983.github.io/spark-pipeline-framework/docs/config-validation) - CI/CD validation
- [Secrets Management](https://dwsmith1983.github.io/spark-pipeline-framework/docs/secrets-management) - Secure credential handling
- [Lifecycle Hooks](https://dwsmith1983.github.io/spark-pipeline-framework/docs/hooks) - Logging, metrics, audit trails
- [Streaming](https://dwsmith1983.github.io/spark-pipeline-framework/docs/streaming) - Structured Streaming support
- [Deployment](https://dwsmith1983.github.io/spark-pipeline-framework/docs/deployment) - Production deployment guides
- [Contributing](https://dwsmith1983.github.io/spark-pipeline-framework/docs/contributing) - Development setup

## Roadmap

Track progress on [GitHub Milestones](https://github.com/dwsmith1983/spark-pipeline-framework/milestones).

| Version | Status | Focus | Key Features |
|---------|--------|-------|--------------|
| **v1.1.0** | Released | Configuration & Streaming | Configuration validation ([#54](https://github.com/dwsmith1983/spark-pipeline-framework/issues/54)), Secrets management ([#57](https://github.com/dwsmith1983/spark-pipeline-framework/issues/57)), Streaming core ([#52](https://github.com/dwsmith1983/spark-pipeline-framework/issues/52)) |
| **v1.2.0** | Planned | Resilience & Contracts | Schema contracts ([#49](https://github.com/dwsmith1983/spark-pipeline-framework/issues/49)), Checkpointing ([#50](https://github.com/dwsmith1983/spark-pipeline-framework/issues/50)), Retry logic ([#55](https://github.com/dwsmith1983/spark-pipeline-framework/issues/55)), Data quality hooks ([#56](https://github.com/dwsmith1983/spark-pipeline-framework/issues/56)) |
| **v2.0.0** | Planned | Parallelism | DAG-based component execution ([#51](https://github.com/dwsmith1983/spark-pipeline-framework/issues/51)) |
| **v2.1.0** | Planned | Streaming Complete | Streaming sources/sinks |
| **v2.2.0** | Planned | Cloud Native | Spark Connect ([#58](https://github.com/dwsmith1983/spark-pipeline-framework/issues/58)) |

## License

Apache 2.0
