# Contributing & Development

This guide covers building the framework from source and setting up a development environment.

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

# Generate ScalaDoc
sbt doc
```

## Cross-Compilation Matrix

The framework cross-compiles for multiple Spark and Scala versions:

| Artifact | Spark | Scala | Java |
|----------|-------|-------|------|
| `*-spark3_2.12` | 3.5.7 | 2.12.18 | 17+ |
| `*-spark3_2.13` | 3.5.7 | 2.13.12 | 17+ |
| `*-spark4_2.13` | 4.0.1 | 2.13.12 | 17+ |

## Pre-commit Hooks

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

The hooks automatically run `scalafmtCheckAll`, `scalastyle`, and `scalafixAll --check` before each commit.

## Manual Linting

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

## Running the Examples

The example module includes comprehensive demonstrations:

### Canonical Batch Example

```bash
# Build the example JAR
sbt examplespark3/package

# Run the canonical batch pipeline example
spark-submit \
  --class io.github.dwsmith1983.pipelines.BatchPipelineExample \
  --master "local[*]" \
  example/target/spark3-jvm-2.13/spark-pipeline-example-spark3_2.13-<version>.jar
```

The BatchPipelineExample demonstrates:
1. Sales data ETL with transformations
2. Aggregation with group-by operations
3. Data enrichment through joins
4. Multi-component pipeline orchestration
5. Lifecycle hooks and metrics collection

### Canonical Streaming Example

```bash
# Run the canonical streaming pipeline example
spark-submit \
  --class io.github.dwsmith1983.pipelines.StreamingPipelineExample \
  --master "local[*]" \
  example/target/spark3-jvm-2.13/spark-pipeline-example-spark3_2.13-<version>.jar
```

See `example/src/main/scala/io/github/dwsmith1983/pipelines/` for:
- `BatchPipelineExample.scala` - **Canonical batch processing example**
- `StreamingPipelineExample.scala` - **Canonical streaming pipeline example**
- `DemoAuditPipeline.scala` - Audit trail demo with security filtering
- `ValidationDemo.scala` - Error handling and validation demo
- `DemoMetricsHooks.scala` - Simple in-memory metrics (for learning/demos)

## Project Structure

```
spark-pipeline-framework/
├── core/           # Traits, config models, instantiation (no Spark dependency)
├── runtime/        # SparkSessionWrapper, DataFlow trait (Spark provided)
├── runner/         # SimplePipelineRunner entry point (Spark provided)
├── example/        # Demo pipelines and components
└── website/        # Docusaurus documentation site
```

## Pull Request Guidelines

1. **PR title**: Follow [conventional commits](https://www.conventionalcommits.org/) format
2. **Tests**: Add/update tests for changes
3. **Linting**: Ensure `sbt scalafmtCheckAll` passes
4. **Tests**: Ensure `sbt test` passes
5. **Docs**: Update documentation if applicable

## Next Steps

- [Getting Started](./getting-started) - Quick start guide
- [Configuration](./configuration) - HOCON configuration reference
- [Components](./components) - Building pipeline components
