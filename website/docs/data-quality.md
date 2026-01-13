# Data Quality Hooks

Data quality hooks allow you to validate data at various points during pipeline execution. Define checks against Spark tables/views and the framework will execute them at the configured times, reporting results and optionally failing the pipeline when thresholds are exceeded.

## Basic Usage

```scala
import io.github.dwsmith1983.spark.pipeline.dq._

val checks = Seq(
  DataQualityCheckConfig(
    RowCountCheck("raw_data", minRows = 1000),
    CheckTiming.AfterComponent("LoadRawData")
  ),
  DataQualityCheckConfig(
    NullCheck("cleaned_data", Seq("user_id"), maxNullPercent = 0.0),
    CheckTiming.AfterComponent("CleanData")
  ),
  DataQualityCheckConfig(
    SchemaCheck("output", Set("id", "name", "timestamp")),
    CheckTiming.AfterPipeline
  )
)

val hooks = DataQualityHooks(spark, checks)
SimplePipelineRunner.run(config, hooks)
```

## Check Timing

Checks can be configured to run at three points in the pipeline lifecycle:

| Timing | When | Use Case |
|--------|------|----------|
| `CheckTiming.BeforePipeline` | Before any component runs | Validate source data, preconditions |
| `CheckTiming.AfterComponent("name")` | After a specific component completes | Validate intermediate results |
| `CheckTiming.AfterPipeline` | After all components complete | Validate final output |

```scala
// Run before pipeline starts
DataQualityCheckConfig(check, CheckTiming.BeforePipeline)

// Run after specific component
DataQualityCheckConfig(check, CheckTiming.AfterComponent("TransformData"))

// Run after pipeline completes
DataQualityCheckConfig(check, CheckTiming.AfterPipeline)
```

## Built-in Checks

### RowCountCheck

Validates that a table has rows within specified bounds.

```scala
// At least 1000 rows
RowCountCheck("orders", minRows = 1000)

// Between 100 and 10000 rows
RowCountCheck("events", minRows = 100, maxRows = Some(10000))

// At least 1 row (default)
RowCountCheck("users")
```

### NullCheck

Validates null percentages in specified columns.

```scala
// No nulls allowed in user_id
NullCheck("users", Seq("user_id"), maxNullPercent = 0.0)

// Allow up to 5% nulls in optional_field
NullCheck("events", Seq("optional_field"), maxNullPercent = 5.0)

// Check multiple columns
NullCheck("orders", Seq("customer_id", "order_date"), maxNullPercent = 0.0)
```

### SchemaCheck

Validates that required columns exist in the table.

```scala
// Ensure required columns are present
SchemaCheck("output", Set("id", "name", "timestamp", "value"))
```

### UniqueCheck

Validates uniqueness of column values (detects duplicates).

```scala
// Single column uniqueness
UniqueCheck("users", Seq("user_id"))

// Composite key uniqueness
UniqueCheck("events", Seq("event_date", "event_id"))

// Allow some duplicates (e.g., up to 1%)
UniqueCheck("logs", Seq("trace_id"), maxDuplicatePercent = 1.0)

// Sample large tables for performance
UniqueCheck("big_table", Seq("id"), sampleFraction = Some(0.1))
```

### RangeCheck

Validates that numeric values fall within a specified range.

```scala
// Age between 0 and 120
RangeCheck("users", column = "age", min = Some(0), max = Some(120))

// Price must be positive
RangeCheck("products", column = "price", min = Some(0.0))

// Percentage must be at most 100
RangeCheck("metrics", column = "completion_pct", max = Some(100.0))
```

### CustomSqlCheck

Executes arbitrary SQL and validates the result.

```scala
// Average order value must exceed threshold
CustomSqlCheck(
  checkName = "avg_order_check",
  tableName = "orders",
  sql = "SELECT AVG(total) as avg_total FROM orders",
  expectation = row => row.getDouble(0) > 50.0
)

// Row count ratio between tables
CustomSqlCheck(
  checkName = "event_user_ratio",
  tableName = "events",
  sql = """
    SELECT
      (SELECT COUNT(*) FROM events) /
      NULLIF((SELECT COUNT(*) FROM users), 0) as ratio
  """,
  expectation = row => row.getDouble(0) >= 1.0,
  failureMessage = Some("Expected at least 1 event per user")
)
```

### CustomCheck

For complex validation logic with full DataFrame access.

```scala
CustomCheck(
  checkName = "complex_validation",
  tableName = "transactions",
  validate = (spark, df) => {
    import spark.implicits._

    // Find invalid records: credit transactions with negative amounts
    val invalidCount = df
      .filter($"amount" < 0 && $"type" === "credit")
      .count()

    if (invalidCount > 0) {
      DataQualityResult.Failed(
        "complex_validation",
        "transactions",
        s"Found $invalidCount credit transactions with negative amounts",
        Map("invalid_count" -> invalidCount)
      )
    } else {
      DataQualityResult.Passed(
        "complex_validation",
        "transactions",
        Map("checked_rows" -> df.count())
      )
    }
  }
)
```

## Failure Modes

Control how the pipeline responds to check failures:

### FailOnError (Default)

Stop pipeline execution immediately when any check fails.

```scala
val hooks = DataQualityHooks(spark, checks, FailureMode.FailOnError)
```

### WarnOnly

Log failures as warnings but allow pipeline to continue. Useful for monitoring without blocking production.

```scala
val hooks = DataQualityHooks(spark, checks, FailureMode.WarnOnly)

// Or use the convenience method
val hooks = DataQualityHooks.warnOnly(spark, checks)
```

### Threshold

Allow up to N check failures before stopping the pipeline.

```scala
// Allow up to 2 failures
val hooks = DataQualityHooks(spark, checks, FailureMode.Threshold(2))
```

## Result Sinks

Configure where check results are written:

### Logging (Default)

Results are logged via Log4j2.

```scala
val hooks = DataQualityHooks(spark, checks)  // Uses logging by default
```

### File Sink

Write results as JSON lines to a file.

```scala
val hooks = DataQualityHooks(
  spark,
  checks,
  FailureMode.WarnOnly,
  DataQualitySink.file("/var/log/dq-results.jsonl")
)

// Or use the convenience method
val hooks = DataQualityHooks.toFile(spark, checks, "/var/log/dq-results.jsonl")
```

Output format (one JSON object per line):
```json
{"status":"passed","check_name":"row_count","table_name":"users","timestamp":"2024-01-15T10:30:00Z","details":{"actual_count":5000,"min_required":1000}}
{"status":"failed","check_name":"null_check","table_name":"orders","message":"Null percentage exceeds 0.0% in: customer_id: 2.50%","timestamp":"2024-01-15T10:30:01Z","details":{}}
```

### Composite Sink

Write to multiple sinks simultaneously.

```scala
val sink = DataQualitySink.compose(
  DataQualitySink.file("/var/log/dq-results.jsonl"),
  DataQualitySink.logging("info")
)

val hooks = DataQualityHooks(spark, checks, FailureMode.WarnOnly, sink)
```

### Custom Sink

Implement `DataQualitySink` for custom destinations.

```scala
class MetricsSink(registry: MeterRegistry) extends DataQualitySink {
  override def write(result: DataQualityResult): Unit = {
    val status = result match {
      case _: DataQualityResult.Passed  => "passed"
      case _: DataQualityResult.Failed  => "failed"
      case _: DataQualityResult.Warning => "warning"
      case _: DataQualityResult.Skipped => "skipped"
    }
    registry.counter("dq.check", "status", status, "check", result.checkName).increment()
  }

  override def flush(): Unit = ()
  override def close(): Unit = ()
}
```

## Accessing Results

After pipeline execution, retrieve check results programmatically:

```scala
val hooks = DataQualityHooks(spark, checks, FailureMode.WarnOnly)
SimplePipelineRunner.run(config, hooks)

// Get all results
val allResults = hooks.getResults

// Get only failures
val failures = hooks.getFailedResults

// Check if all passed
if (hooks.allChecksPassed) {
  println("All data quality checks passed!")
} else {
  println(s"${failures.size} checks failed")
  failures.foreach { f =>
    println(s"  - ${f.checkName} on ${f.tableName}: ${f.message}")
  }
}
```

## Combining with Other Hooks

Compose data quality hooks with other hooks:

```scala
import io.github.dwsmith1983.spark.pipeline.config.PipelineHooks

val combined = PipelineHooks.compose(
  DataQualityHooks(spark, checks, FailureMode.WarnOnly),
  MetricsHooks(prometheusRegistry),
  AuditHooks.toFile("/var/log/audit.jsonl"),
  LoggingHooks.structured()
)

SimplePipelineRunner.run(config, combined)
```

## Best Practices

1. **Check early, check often**: Run validation after each major transformation step, not just at the end.

2. **Use appropriate thresholds**: Set `maxNullPercent` and `maxDuplicatePercent` based on actual data characteristics.

3. **Sample large tables**: Use `sampleFraction` in `UniqueCheck` for tables with billions of rows to avoid OOM.

4. **Meaningful check names**: Use descriptive names for `CustomSqlCheck` and `CustomCheck` to make logs readable.

5. **Use WarnOnly in development**: Start with `WarnOnly` to understand data patterns, then switch to `FailOnError` for production.

6. **Persist results**: Use `DataQualitySink.file()` to keep historical records of data quality metrics.

7. **Handle missing tables gracefully**: Checks return `Skipped` when tables don't exist - this is logged but doesn't fail the pipeline.

## Example: Complete Pipeline

```scala
import io.github.dwsmith1983.spark.pipeline.dq._
import io.github.dwsmith1983.spark.pipeline.config._
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner

// Define comprehensive checks
val checks = Seq(
  // Validate source data
  DataQualityCheckConfig(
    RowCountCheck("raw_events", minRows = 10000),
    CheckTiming.AfterComponent("LoadEvents")
  ),
  DataQualityCheckConfig(
    NullCheck("raw_events", Seq("event_id", "user_id", "timestamp"), maxNullPercent = 0.0),
    CheckTiming.AfterComponent("LoadEvents")
  ),

  // Validate after deduplication
  DataQualityCheckConfig(
    UniqueCheck("deduped_events", Seq("event_id")),
    CheckTiming.AfterComponent("DeduplicateEvents")
  ),

  // Validate enriched data
  DataQualityCheckConfig(
    SchemaCheck("enriched_events", Set("event_id", "user_id", "user_name", "timestamp")),
    CheckTiming.AfterComponent("EnrichWithUserData")
  ),

  // Validate final output
  DataQualityCheckConfig(
    RangeCheck("aggregated_metrics", column = "event_count", min = Some(0)),
    CheckTiming.AfterPipeline
  ),
  DataQualityCheckConfig(
    CustomSqlCheck(
      "completeness_check",
      "aggregated_metrics",
      "SELECT COUNT(DISTINCT user_id) FROM aggregated_metrics",
      row => row.getLong(0) > 0,
      Some("Output must contain at least one user")
    ),
    CheckTiming.AfterPipeline
  )
)

// Create hooks with file output
val dqHooks = DataQualityHooks(
  spark,
  checks,
  FailureMode.Threshold(2),  // Allow up to 2 failures
  DataQualitySink.file("/var/log/pipeline-dq.jsonl")
)

// Run pipeline with DQ validation
SimplePipelineRunner.run(pipelineConfig, dqHooks)

// Report results
if (!dqHooks.allChecksPassed) {
  println(s"WARNING: ${dqHooks.getFailedResults.size} data quality checks failed")
}
```
