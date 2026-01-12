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

## Running the Demo

The example module includes a complete end-to-end demo:

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
[RUNNING] Executing pipeline with composed hooks...

[PIPELINE START] Word Analysis Pipeline (2 components)
  [1/2] Starting: WordCount(all words)
  [1/2] Completed: WordCount(all words) (1842ms)
  [2/2] Starting: WordCount(frequent words only)
  [2/2] Completed: WordCount(frequent words only) (211ms)

[PIPELINE COMPLETE] 2 components executed in 2395ms
======================================================================
```

See `example/src/main/scala/io/github/dwsmith1983/pipelines/` for:
- `WordCount.scala` - Example component
- `DemoMetricsHooks.scala` - Simple in-memory metrics (for learning/demos)
- `DemoPipeline.scala` - End-to-end runnable demo with metrics
- `DemoAuditPipeline.scala` - Audit trail demo with security filtering

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
