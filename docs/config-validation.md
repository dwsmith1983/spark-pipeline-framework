# Configuration Validation

Validate pipeline configurations before execution. Catch errors early in CI/CD pipelines without starting Spark or instantiating components.

## Quick Start

Validate configuration from a file:

```scala
import io.github.dwsmith1983.spark.pipeline.config._

val result = ConfigValidator.validateFromFile("pipeline.conf")

result match {
  case ValidationResult.Valid(name, count, warnings) =>
    println(s"Pipeline '$name' is valid with $count components")
    warnings.foreach(w => println(s"Warning: ${w.message}"))

  case ValidationResult.Invalid(errors, warnings) =>
    errors.foreach(e => println(s"Error: ${e.fullMessage}"))
    sys.exit(1)
}
```

## Validation Methods

| Method | Input | Description |
|--------|-------|-------------|
| `validate(config)` | `Config` object | Validate parsed HOCON Config |
| `validateFromString(hocon)` | HOCON string | Parse and validate |
| `validateFromFile(path)` | File path | Read, parse, and validate |

All methods accept an optional `ValidationOptions` parameter.

## Validation Phases

Validation proceeds through five phases, stopping at the first error in each phase:

| Phase | What's Checked |
|-------|---------------|
| `ConfigSyntax` | HOCON syntax correctness |
| `RequiredFields` | `pipeline` block, `pipeline-name`, `pipeline-components` |
| `TypeResolution` | Component classes exist on classpath |
| `ComponentConfig` | Companion objects extend `ConfigurableInstance` |
| `ResourceValidation` | (Optional) Referenced paths/tables exist |

## ValidationResult

Results are either `Valid` or `Invalid`:

```scala
sealed trait ValidationResult {
  def isValid: Boolean
  def isInvalid: Boolean
  def errors: List[ValidationError]
  def warnings: List[ValidationWarning]
}

// Valid result
ValidationResult.Valid(
  pipelineName = "My Pipeline",
  componentCount = 3,
  warnings = List.empty
)

// Invalid result
ValidationResult.Invalid(
  errors = List(ValidationError(...)),
  warnings = List.empty
)
```

### Combining Results

Combine multiple validation results:

```scala
val result1 = ConfigValidator.validate(config1)
val result2 = ConfigValidator.validate(config2)

val combined = result1 ++ result2  // Accumulates errors and warnings
```

## Error Details

Each `ValidationError` includes:

```scala
case class ValidationError(
  phase: ValidationPhase,      // Which phase failed
  location: ErrorLocation,     // Pipeline or specific component
  message: String,             // Human-readable description
  cause: Option[Throwable]     // Underlying exception
) {
  def fullMessage: String  // Formatted: "[phase] location: message"
}
```

### Error Locations

Errors pinpoint where the problem occurred:

```scala
// Pipeline-level error
ErrorLocation.Pipeline
// Output: "pipeline configuration"

// Component-specific error
ErrorLocation.Component(
  index = 2,
  instanceType = "com.example.MyComponent",
  instanceName = "my-component"
)
// Output: "component[2] 'my-component' (com.example.MyComponent)"
```

## Validation Options

Control validation behavior:

```scala
case class ValidationOptions(
  validateResources: Boolean = false  // Check paths/tables exist
)

// Fast validation (default)
ConfigValidator.validate(config)

// With resource validation
ConfigValidator.validate(config, ValidationOptions(validateResources = true))
```

## Common Validation Errors

### Missing Pipeline Block

```
[required-fields] pipeline configuration: Missing required 'pipeline' block in configuration
```

**Fix:** Ensure your config has a `pipeline { ... }` block.

### Empty Components List

```
[required-fields] pipeline configuration: Pipeline must have at least one component in 'pipeline-components'
```

**Fix:** Add at least one component to `pipeline-components`.

### Class Not Found

```
[type-resolution] component[0] 'MyComponent' (com.example.MyComponent): Class not found: com.example.MyComponent. Ensure the class is on the classpath.
```

**Fix:** Verify the class name is correct and the JAR is on the classpath.

### Missing Companion Object

```
[type-resolution] component[0] 'MyComponent' (com.example.MyComponent): Companion object not found for com.example.MyComponent. Ensure it has a companion object extending ConfigurableInstance.
```

**Fix:** Add a companion object that extends `ConfigurableInstance`:

```scala
object MyComponent extends ConfigurableInstance {
  override def createFromConfig(conf: Config): MyComponent = ???
}
```

### Invalid ConfigurableInstance

```
[component-config] component[0] 'MyComponent' (com.example.MyComponent): Companion object of com.example.MyComponent does not extend ConfigurableInstance
```

**Fix:** Ensure the companion object extends `ConfigurableInstance`.

## CI/CD Integration

### Exit Codes

Use validation results to set exit codes:

```scala
object ValidatePipeline extends App {
  val configPath = args.headOption.getOrElse("pipeline.conf")

  ConfigValidator.validateFromFile(configPath) match {
    case ValidationResult.Valid(name, count, warnings) =>
      println(s"OK: Pipeline '$name' with $count components")
      warnings.foreach(w => System.err.println(s"WARN: ${w.fullMessage}"))
      sys.exit(0)

    case ValidationResult.Invalid(errors, warnings) =>
      errors.foreach(e => System.err.println(s"ERROR: ${e.fullMessage}"))
      warnings.foreach(w => System.err.println(s"WARN: ${w.fullMessage}"))
      sys.exit(1)
  }
}
```

### GitHub Actions Example

```yaml
- name: Validate Pipeline Config
  run: |
    sbt "runMain com.example.ValidatePipeline configs/production.conf"
```

## Validation vs Dry Run

| Feature | Validation | Dry Run |
|---------|-----------|---------|
| Speed | Fast | Slower |
| Spark Required | No | Yes |
| Component Instantiation | No | Yes |
| Config Parsing | Structure only | Full parsing |
| Use Case | CI/CD pre-flight | Pre-production check |

Use validation for quick CI checks. Use dry-run when you need to verify components can be fully instantiated with their configuration.

## Programmatic Usage

### Validate Config Object

```scala
import com.typesafe.config.ConfigFactory
import io.github.dwsmith1983.spark.pipeline.config._

val config = ConfigFactory.parseString("""
  pipeline {
    pipeline-name = "Test Pipeline"
    pipeline-components = [
      {
        instance-type = "com.example.MySource"
        instance-name = "source"
        instance-config { path = "/data/input" }
      }
    ]
  }
""")

val result = ConfigValidator.validate(config)
```

### Iterate Over Errors

```scala
result match {
  case ValidationResult.Invalid(errors, _) =>
    errors.foreach { error =>
      println(s"Phase: ${error.phase.name}")
      println(s"Location: ${error.location.description}")
      println(s"Message: ${error.message}")
      error.cause.foreach(_.printStackTrace())
    }
  case _ =>
}
```

### Custom Validation

Extend validation with custom checks:

```scala
def validateWithCustomRules(config: Config): ValidationResult = {
  val baseResult = ConfigValidator.validate(config)

  // Add custom validation
  val customErrors = checkCustomRules(config)

  if (customErrors.nonEmpty) {
    baseResult ++ ValidationResult.Invalid(customErrors)
  } else {
    baseResult
  }
}
```
