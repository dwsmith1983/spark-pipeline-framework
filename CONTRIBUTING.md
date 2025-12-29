# Contributing to spark-pipeline-framework

Thank you for your interest in contributing! This guide will help you get started.

Please note that this project is released with a [Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.

## Development Setup

### Prerequisites

- **Java 17+** (required by Spark 3.5.x and 4.x)
- **SBT 1.9+** ([installation guide](https://www.scala-sbt.org/download.html))
- **Scala** (managed by SBT, no separate installation needed)

### Clone and Build

```bash
# Clone the repository
git clone https://github.com/dwsmith1983/spark-pipeline-framework.git
cd spark-pipeline-framework

# Compile all modules
sbt compile

# Run tests
sbt test

# Run tests with coverage
sbt coverage test coverageReport
```

### IDE Setup

**IntelliJ IDEA** (recommended):
1. Install the Scala plugin
2. Open the project directory
3. Import as an SBT project
4. Wait for indexing to complete

**VS Code**:
1. Install the Metals extension
2. Open the project directory
3. Allow Metals to import the build

### Pre-commit Hooks

Set up pre-commit hooks to catch formatting issues before pushing:

```bash
# Option 1: Using pre-commit framework (recommended)
pip install pre-commit
pre-commit install

# Option 2: Manual git hook
cp scripts/pre-commit.sh .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

## Project Structure

```
spark-pipeline-framework/
├── core/           # Config models, traits (no Spark dependency)
├── runtime/        # SparkSession management, DataFlow trait
├── runner/         # SimplePipelineRunner entry point
├── example/        # Reference implementations (WordCount, etc.)
└── project/        # SBT build configuration
```

## Making Changes

### Branching Strategy

- `main` - stable, release-ready code
- `feat/*` - new features
- `fix/*` - bug fixes
- `docs/*` - documentation updates
- `chore/*` - maintenance tasks

### Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/) for automatic versioning:

```
feat: add new feature        → bumps minor version (post-1.0.0)
fix: fix a bug               → bumps patch version
docs: update documentation   → no version bump
chore: maintenance task      → no version bump
```

Breaking changes:
```
feat!: breaking change       → bumps major version (post-1.0.0)
```

### Code Style

- **Formatting**: Run `sbt scalafmtAll` before committing
- **Style checks**: Run `sbt scalastyle` to check for style issues
- **Compiler warnings**: All warnings are treated as errors (`-Xfatal-warnings`)

### Testing

```bash
# Run all tests
sbt test

# Run tests for a specific module
sbt "project core" test
sbt "project runnerspark3" test

# Run tests with coverage
sbt coverage test coverageReport

# Run a specific test class
sbt "testOnly *SimplePipelineRunnerSpec"
```

Coverage minimum is 75%. Tests must pass before merging.

### Cross-Compilation

The project supports multiple Spark and Scala versions:

| Configuration | Spark | Scala |
|--------------|-------|-------|
| `*spark32_12` | 3.5.7 | 2.12 |
| `*spark3` | 3.5.7 | 2.13 |
| `*spark4` | 4.0.1 | 2.13 |

To test a specific configuration:
```bash
sbt "project runnerspark32_12" test
sbt "project runnerspark3" test
sbt "project runnerspark4" test
```

## Pull Request Process

1. **Create a branch** from `main`
2. **Make your changes** with tests
3. **Run checks locally**:
   ```bash
   sbt scalafmtCheckAll scalastyle test
   ```
4. **Push and create a PR** against `main`
5. **Wait for CI** - all checks must pass
6. **Address review feedback** if any
7. **Merge** - maintainers will merge when ready

### PR Title Format

Use conventional commit format for PR titles:
- `feat: add retry configuration support`
- `fix: handle empty pipeline components`
- `docs: improve getting started guide`

## Adding a New Component Example

To add a new example component:

1. Create the component in `example/src/main/scala/`:

```scala
package io.github.dwsmith1983.pipelines

import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow
import com.typesafe.config.Config
import pureconfig._
import pureconfig.generic.auto._

object MyComponent extends ConfigurableInstance {
  case class Config(inputPath: String, outputPath: String)

  override def createFromConfig(conf: Config): MyComponent =
    new MyComponent(ConfigSource.fromConfig(conf).loadOrThrow[Config])
}

class MyComponent(conf: MyComponent.Config) extends DataFlow {
  override def run(): Unit = {
    logInfo(s"Processing ${conf.inputPath}")
    // Your Spark logic here
  }
}
```

2. Add tests in `example/src/test/scala/`
3. Update `example-pipeline.conf` if appropriate

## Reporting Issues

When reporting bugs, please include:
- Spark version and Scala version
- Minimal configuration to reproduce
- Full stack trace if applicable
- Expected vs actual behavior

## Questions?

- Open a [GitHub Discussion](https://github.com/dwsmith1983/spark-pipeline-framework/discussions)
- Check existing issues for similar questions

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
