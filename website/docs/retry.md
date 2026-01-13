# Retry Logic

spark-pipeline-framework provides built-in retry logic for handling transient failures in pipeline components. This feature allows components to automatically recover from temporary errors without manual intervention.

## Overview

The retry system consists of two main features:

1. **Retry Policy** - Configurable retry behavior with exponential backoff and jitter
2. **Circuit Breaker** - Protection against cascading failures (optional)

Both features integrate seamlessly with [PipelineHooks](./hooks.md) for observability.

## Quick Start

Add retry configuration to your pipeline:

```hocon
pipeline {
  pipeline-name = "My Resilient Pipeline"

  retry-policy {
    max-retries = 3
    initial-delay-ms = 1000
    max-delay-ms = 30000
    backoff-multiplier = 2.0
    jitter-factor = 0.1
  }

  pipeline-components = [
    {
      instance-type = "com.example.FlakeyApiComponent"
      instance-name = "External API Call"
      instance-config { ... }
    }
  ]
}
```

## Retry Policy Configuration

### Pipeline-Level Configuration

Set a default retry policy for all components:

```hocon
pipeline {
  pipeline-name = "Pipeline with Retry"

  retry-policy {
    max-retries = 3           # Number of retry attempts (0 = no retries)
    initial-delay-ms = 1000   # First retry delay in milliseconds
    max-delay-ms = 30000      # Maximum delay cap
    backoff-multiplier = 2.0  # Exponential backoff multiplier
    jitter-factor = 0.1       # Random jitter (0.1 = ±10%)
  }

  pipeline-components = [ ... ]
}
```

### Component-Level Override

Override the pipeline-level policy for specific components:

```hocon
pipeline {
  pipeline-name = "Mixed Retry Policies"

  retry-policy {
    max-retries = 2
    initial-delay-ms = 500
  }

  pipeline-components = [
    {
      instance-type = "com.example.FastComponent"
      instance-name = "Fast Retry"
      instance-config { ... }
      # Uses pipeline-level retry policy (2 retries, 500ms)
    },
    {
      instance-type = "com.example.SlowApiComponent"
      instance-name = "Slow External API"
      instance-config { ... }
      retry-policy {
        max-retries = 5           # More retries for flaky API
        initial-delay-ms = 2000   # Longer initial delay
        max-delay-ms = 60000
      }
    },
    {
      instance-type = "com.example.CriticalComponent"
      instance-name = "No Retry Allowed"
      instance-config { ... }
      retry-policy {
        max-retries = 0           # Disable retry for this component
      }
    }
  ]
}
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max-retries` | Int | 3 | Maximum retry attempts. 0 disables retry. |
| `initial-delay-ms` | Long | 1000 | Delay before first retry (milliseconds) |
| `max-delay-ms` | Long | 30000 | Maximum delay cap for exponential backoff |
| `backoff-multiplier` | Double | 2.0 | Multiplier for exponential backoff |
| `jitter-factor` | Double | 0.1 | Random jitter factor (0.0 to 1.0) |
| `retryable-exceptions` | List[String] | [] | Exception classes to retry. Empty = all exceptions |

### Backoff Calculation

The delay before retry attempt `n` is calculated as:

```
baseDelay = min(initialDelayMs * (backoffMultiplier ^ n), maxDelayMs)
jitter = baseDelay * jitterFactor * random(-1, 1)
delay = max(0, baseDelay + jitter)
```

**Example with defaults** (initialDelay=1000, multiplier=2.0, maxDelay=30000):

| Attempt | Base Delay | With 10% Jitter |
|---------|------------|-----------------|
| 1 | 1000ms | 900-1100ms |
| 2 | 2000ms | 1800-2200ms |
| 3 | 4000ms | 3600-4400ms |
| 4 | 8000ms | 7200-8800ms |
| 5+ | 16000ms+ | capped at 30000ms |

## Circuit Breaker (Optional)

The circuit breaker pattern prevents cascading failures by temporarily stopping requests to a failing component.

### Configuration

```hocon
pipeline {
  pipeline-name = "Pipeline with Circuit Breaker"

  circuit-breaker {
    failure-threshold = 5        # Failures before opening circuit
    reset-timeout-ms = 60000     # Time before trying half-open
    half-open-success-threshold = 2  # Successes to close circuit
  }

  pipeline-components = [ ... ]
}
```

### Circuit Breaker States

```
CLOSED ──[failure threshold reached]──► OPEN
   ▲                                       │
   │                                       │
   │                              [reset timeout elapsed]
   │                                       │
   │                                       ▼
   └──[success threshold reached]──── HALF_OPEN
                                           │
                                    [any failure]
                                           │
                                           ▼
                                         OPEN
```

| State | Behavior |
|-------|----------|
| **CLOSED** | Normal operation. Requests flow through. Failures are counted. |
| **OPEN** | Circuit is tripped. Requests fail immediately with `CircuitBreakerOpenException`. |
| **HALF_OPEN** | Testing recovery. Limited requests allowed. Success closes circuit, failure re-opens. |

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `failure-threshold` | Int | 5 | Consecutive failures before opening |
| `reset-timeout-ms` | Long | 60000 | Time before attempting half-open |
| `half-open-success-threshold` | Int | 2 | Successes needed to close from half-open |

## Observability

### Retry Hooks

The retry system integrates with `PipelineHooks` for monitoring:

```scala
class MyRetryHooks extends PipelineHooks {
  override def onRetryAttempt(
    config: ComponentConfig,
    attempt: Int,
    maxAttempts: Int,
    delayMs: Long,
    error: Throwable
  ): Unit = {
    println(s"Retry $attempt/$maxAttempts for ${config.instanceName}")
    println(s"Error: ${error.getMessage}, waiting ${delayMs}ms")
  }

  override def onCircuitBreakerStateChange(
    componentName: String,
    oldState: CircuitState,
    newState: CircuitState
  ): Unit = {
    println(s"Circuit breaker for $componentName: ${oldState.name} -> ${newState.name}")
  }
}
```

### Built-in Hooks Support

#### LoggingHooks

Automatically logs retry attempts in structured JSON or human-readable format:

```json
{"event":"component_retry","run_id":"abc123","component_name":"FlakeyApi",
 "attempt":1,"max_attempts":3,"delay_ms":1000,"error_type":"IOException",
 "error_message":"Connection timeout","timestamp":"2024-01-15T10:30:00Z"}
```

#### MetricsHooks

Records retry metrics via Micrometer:

- `pipeline.component.retries` - Counter of retry attempts
- `pipeline.component.retry_attempts` - Counter by attempt number
- `pipeline.circuit_breaker.state` - Gauge of circuit state (0=closed, 1=half_open, 2=open)
- `pipeline.circuit_breaker.transitions` - Counter of state transitions

## Best Practices

### When to Use Retry

**Good candidates for retry:**
- Network calls (HTTP APIs, database connections)
- Cloud service calls (S3, BigQuery, etc.)
- Resource contention issues
- Transient infrastructure failures

**Poor candidates for retry:**
- Validation errors (bad input data)
- Authentication failures
- Business logic errors
- Out of memory errors

### Filtering Retryable Exceptions

Specify which exceptions should trigger retry:

```hocon
retry-policy {
  max-retries = 3
  initial-delay-ms = 1000
  retryable-exceptions = [
    "java.io.IOException",
    "java.net.SocketException",
    "com.example.TransientApiException"
  ]
}
```

When `retryable-exceptions` is empty (default), all exceptions trigger retry.

### Combining with failFast

Retry integrates with the `fail-fast` setting:

```hocon
pipeline {
  fail-fast = false  # Continue to next component after retries exhausted

  retry-policy {
    max-retries = 3
  }

  pipeline-components = [
    { ... },  # If this fails after 3 retries, continue to next
    { ... },  # This will still run
  ]
}
```

With `fail-fast = true` (default), the pipeline stops after a component exhausts all retries.

### Tuning Guidelines

1. **Start conservative** - Begin with fewer retries and shorter delays
2. **Add jitter** - Prevents thundering herd when multiple pipelines retry simultaneously
3. **Set reasonable max delay** - Don't let backoff grow unbounded
4. **Monitor retry rates** - High retry rates indicate underlying issues
5. **Use circuit breaker for external dependencies** - Protects downstream services

## Programmatic Configuration

Create retry policies in Scala code:

```scala
import io.github.dwsmith1983.spark.pipeline.config._

// Use preset policies
val noRetry = RetryPolicy.NoRetry
val defaultRetry = RetryPolicy.Default
val aggressiveRetry = RetryPolicy.Aggressive

// Custom policy
val customRetry = RetryPolicy(
  maxRetries = 5,
  initialDelayMs = 500L,
  maxDelayMs = 60000L,
  backoffMultiplier = 2.0,
  jitterFactor = 0.2,
  retryableExceptions = Set("java.io.IOException")
)

// Circuit breaker
val circuitBreaker = CircuitBreakerConfig(
  failureThreshold = 3,
  resetTimeoutMs = 30000L,
  halfOpenSuccessThreshold = 2
)

// Build pipeline config
val pipelineConfig = PipelineConfig(
  pipelineName = "Resilient Pipeline",
  pipelineComponents = components,
  retryPolicy = Some(customRetry),
  circuitBreaker = Some(circuitBreaker)
)
```

## Error Handling

### CircuitBreakerOpenException

When the circuit breaker is open, components fail immediately with:

```scala
class CircuitBreakerOpenException(
  val componentName: String,
  val state: CircuitState
) extends RuntimeException
```

This exception is **not retryable** - it propagates immediately to prevent further load on failing components.

### Retry Exhaustion

When all retries are exhausted:

1. `onComponentFailure` hook is called with the final exception
2. If `fail-fast = true`, pipeline stops and throws the exception
3. If `fail-fast = false`, failure is recorded and pipeline continues

## Complete Example

```hocon
spark {
  master = "yarn"
  app-name = "Resilient Data Pipeline"
}

pipeline {
  pipeline-name = "ETL with Retry"
  fail-fast = false

  # Global retry policy
  retry-policy {
    max-retries = 3
    initial-delay-ms = 1000
    max-delay-ms = 30000
    backoff-multiplier = 2.0
    jitter-factor = 0.1
  }

  # Circuit breaker for external calls
  circuit-breaker {
    failure-threshold = 5
    reset-timeout-ms = 60000
  }

  pipeline-components = [
    {
      instance-type = "com.example.S3DataLoader"
      instance-name = "Load from S3"
      instance-config {
        bucket = "my-data-bucket"
        prefix = "raw/"
      }
      # Uses global retry policy
    },
    {
      instance-type = "com.example.DataTransformer"
      instance-name = "Transform Data"
      instance-config { ... }
      retry-policy {
        max-retries = 0  # No retry for transformation
      }
    },
    {
      instance-type = "com.example.ExternalApiEnricher"
      instance-name = "Enrich from API"
      instance-config {
        api-url = "https://api.example.com"
      }
      retry-policy {
        max-retries = 5        # More retries for flaky API
        initial-delay-ms = 2000
        retryable-exceptions = [
          "java.io.IOException",
          "java.net.SocketTimeoutException"
        ]
      }
    },
    {
      instance-type = "com.example.BigQueryWriter"
      instance-name = "Write to BigQuery"
      instance-config {
        project = "my-project"
        dataset = "analytics"
        table = "enriched_data"
      }
      # Uses global retry policy
    }
  ]
}
```
