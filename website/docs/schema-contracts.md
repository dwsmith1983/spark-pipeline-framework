# Schema Contracts

Schema contracts allow pipeline components to declare their expected input and output schemas. When enabled, the framework validates schema compatibility between adjacent components at runtime, catching mismatches early with clear error messages.

## Overview

By default, components don't declare or validate their schemas. If Component A outputs a DataFrame missing columns that Component B expects, the pipeline fails at runtime with unclear errors deep in component execution.

Schema contracts solve this by:

- Allowing components to optionally declare input/output schemas
- Validating schema compatibility between adjacent components before execution
- Providing clear error messages when schemas don't match

## Basic Usage

### Declaring Schema Contracts

Extend the `SchemaContract` trait and implement `inputContract` and/or `outputContract`:

```scala
import io.github.dwsmith1983.spark.pipeline.config.{SchemaContract, SchemaDefinition, SchemaField}
import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow

class MyTransform(conf: MyTransform.Config) extends DataFlow with SchemaContract {

  // Declare expected input schema
  override def inputContract: Option[SchemaDefinition] = Some(
    SchemaDefinition(Seq(
      SchemaField("user_id", "string", nullable = false),
      SchemaField("transaction_date", "date"),
      SchemaField("amount", "double")
    ))
  )

  // Declare produced output schema
  override def outputContract: Option[SchemaDefinition] = Some(
    SchemaDefinition(Seq(
      SchemaField("user_id", "string", nullable = false),
      SchemaField("transaction_date", "date"),
      SchemaField("amount", "double"),
      SchemaField("processed_at", "timestamp")  // Added field
    ))
  )

  override def run(): Unit = {
    // Implementation
  }
}
```

### Enabling Validation

Enable schema validation in your pipeline configuration:

```hocon
pipeline {
  pipeline-name = "My Data Pipeline"

  schema-validation {
    enabled = true
  }

  pipeline-components = [
    { ... }
  ]
}
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `enabled` | `false` | Enable schema validation between components |
| `strict` | `false` | Require all components to have schema contracts |
| `fail-on-warning` | `false` | Treat validation warnings as errors |

### Example Configuration

```hocon
pipeline {
  pipeline-name = "Strict Schema Pipeline"

  schema-validation {
    enabled = true
    strict = true           # All components must declare schemas
    fail-on-warning = true  # Warnings become errors
  }

  pipeline-components = [...]
}
```

## Supported Data Types

Schema contracts use string-based data types that map to Spark types:

| String | Spark Type |
|--------|------------|
| `string` | `StringType` |
| `integer`, `int` | `IntegerType` |
| `long`, `bigint` | `LongType` |
| `double` | `DoubleType` |
| `float` | `FloatType` |
| `boolean`, `bool` | `BooleanType` |
| `timestamp` | `TimestampType` |
| `date` | `DateType` |
| `binary` | `BinaryType` |
| `byte`, `tinyint` | `ByteType` |
| `short`, `smallint` | `ShortType` |
| `decimal(p,s)` | `DecimalType(p,s)` |

## Validation Rules

### Compatibility Checking

For a producer's output to be compatible with a consumer's input:

1. **All required fields must exist**: Every field in the consumer's input contract must exist in the producer's output contract
2. **Types must match**: Field types must be identical (case-insensitive comparison)
3. **Nullability must be satisfied**: Non-nullable consumer fields cannot be fed by nullable producer fields

### Validation Behavior

| Scenario | Behavior |
|----------|----------|
| Neither component has contracts | Skip validation |
| Only producer has contract | Warning (unless `fail-on-warning`) |
| Only consumer has contract | Warning (unless `fail-on-warning`) |
| Both have contracts | Validate compatibility |

### Strict Mode

When `strict = true`:

- All components must declare schema contracts
- Validation fails if any component lacks a contract
- Useful for production pipelines where schema guarantees are required

## Error Messages

When validation fails, you get clear error messages:

```
Schema contract violation between 'ExtractSales' and 'TransformSales':
  - Missing field 'customer_id' is missing from producer output
    (field: customer_id, expected: string)
  - Type mismatch for field 'amount'
    (field: amount, expected: decimal(10,2), actual: double)
```

## Convenience Methods

### SchemaDefinition.of

Create schemas concisely using tuples:

```scala
val schema = SchemaDefinition.of(
  ("user_id", "string", false),   // (name, type, nullable)
  ("amount", "double", true)
)
```

### SchemaField Constants

Use predefined type constants:

```scala
import io.github.dwsmith1983.spark.pipeline.config.SchemaField.DataTypes._

SchemaField("id", Long, nullable = false)
SchemaField("name", String)
SchemaField("created", Timestamp)
```

## Integration with Spark

The `SparkSchemaConverter` utility converts between schema contracts and Spark's `StructType`:

```scala
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSchemaConverter

// Convert schema definition to Spark StructType
val structType = SparkSchemaConverter.toStructType(schemaDefinition)

// Convert Spark schema to schema definition
val schemaDefinition = SparkSchemaConverter.fromStructType(df.schema)

// Validate DataFrame against expected schema
val errors = SparkSchemaConverter.validateAgainst(expectedSchema, df.schema)
if (errors.nonEmpty) {
  logger.error(s"Schema mismatch: ${errors.mkString(", ")}")
}
```

## Best Practices

### 1. Start with Output Contracts

Begin by adding `outputContract` to components that produce data. This documents what the component guarantees to produce.

### 2. Add Input Contracts Gradually

Add `inputContract` to components that have specific requirements. This catches mismatches early.

### 3. Use Strict Mode in Production

Enable `strict = true` for production pipelines to ensure all components have explicit contracts.

### 4. Keep Schemas Minimal

Only declare fields that matter for validation. Extra fields in the producer output don't cause errors (just warnings).

### 5. Document with Descriptions

Add descriptions to schemas for documentation:

```scala
SchemaDefinition(
  fields = Seq(SchemaField("user_id", "string")),
  description = Some("User event data after deduplication")
)
```

## Backward Compatibility

Schema contracts are fully backward compatible:

- Components without contracts continue to work unchanged
- Validation is disabled by default
- Existing pipelines require no changes

To adopt schema contracts incrementally:

1. Add `SchemaContract` trait to components one at a time
2. Enable validation with `strict = false` initially
3. Address warnings as they appear
4. Enable `strict = true` when all components have contracts
