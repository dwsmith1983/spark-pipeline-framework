# Scope & Design

This page describes what Spark Pipeline Framework is designed for, its intentional limitations, and how it compares to alternative approaches.

## Design Philosophy

Spark Pipeline Framework is built on a single principle: **configuration-driven simplicity**.

The framework enables teams to build production Spark batch pipelines using HOCON configuration files, with minimal boilerplate and maximum clarity. It prioritizes:

- **Simplicity over flexibility** - Sequential execution, not DAGs
- **Configuration over code** - Pipelines defined in HOCON, not Scala/Python
- **Composition over inheritance** - Small, focused components assembled via config
- **Observability by default** - Built-in hooks for logging, metrics, and audit

## What This Framework IS

### Simple Sequential Pipelines

Spark Pipeline Framework executes components **sequentially, in order**. This is intentional:

```
Component A → Component B → Component C → Done
```

This model works well for:

- **ETL pipelines** with clear input → transform → output stages
- **Data ingestion** from source systems to data lakes
- **Feature engineering** pipelines with ordered transformations
- **Report generation** with sequential aggregations
- **Data quality** pipelines with validation steps

### Configuration-Driven Architecture

Pipelines are defined entirely in HOCON configuration:

```json
pipeline {
  pipeline-name = "Daily Sales ETL"
  pipeline-components = [
    { instance-type = "com.company.ExtractSales", ... },
    { instance-type = "com.company.TransformSales", ... },
    { instance-type = "com.company.LoadToWarehouse", ... }
  ]
}
```

Benefits:
- **No recompilation** to change pipeline structure
- **Environment-specific configs** via HOCON includes
- **Auditable changes** via version control
- **Non-developers can modify** pipeline parameters

### Production-Ready Observability

Built-in hooks provide enterprise-grade observability:

| Hook | Purpose |
|------|---------|
| `LoggingHooks` | Structured JSON logging with correlation IDs |
| `MetricsHooks` | Micrometer integration (Prometheus, Datadog, etc.) |
| `AuditHooks` | Persistent audit trail with security filtering |

### Flexible Error Handling

Two error handling modes:

- **Fail-fast** (default): Stop on first error
- **Continue-on-error**: Run all components, collect failures

```json
pipeline {
  fail-fast = false  // Continue even if components fail
}
```

## What This Framework is NOT

### Not a DAG Executor

The framework does **not** support:

- Parallel component execution
- Dependency graphs between components
- Conditional branching based on component outputs
- Dynamic pipeline modification at runtime

**If you need DAGs**, consider:
- Apache Airflow
- Prefect
- Dagster
- Apache Argo Workflows

### Not a Streaming Framework

This framework is for **batch processing only**. It does not support:

- Structured Streaming
- Continuous processing
- Micro-batch streaming
- Real-time data ingestion

**If you need streaming**, use:
- Spark Structured Streaming directly
- Apache Flink
- Apache Kafka Streams

### Not a Schema Validation Framework

Components do not declare or validate their input/output schemas. There is:

- No compile-time schema checking between components
- No runtime schema validation
- No automatic schema evolution handling

**For schema validation**, combine with:
- Great Expectations
- Deequ
- Custom validation components

### Not a Workflow Orchestrator

The framework runs a single pipeline to completion. It does not:

- Schedule pipelines
- Manage dependencies between pipelines
- Retry failed pipelines automatically
- Provide a web UI for monitoring

**For orchestration**, use:
- Apache Airflow
- Prefect
- Dagster
- AWS Step Functions
- Kubernetes CronJobs

## When to Use This Framework

### Good Fit

| Scenario | Why It Works |
|----------|--------------|
| Simple ETL pipelines | Sequential stages, clear flow |
| Data migration jobs | Ordered steps, config-driven |
| Batch report generation | Predictable execution order |
| Feature engineering | Transform chains without branches |
| Data quality pipelines | Sequential validation checks |

### Not a Good Fit

| Scenario | Better Alternative |
|----------|-------------------|
| Complex DAG workflows | Airflow, Prefect, Dagster |
| Real-time streaming | Spark Streaming, Flink |
| ML training pipelines | MLflow, Kubeflow, SageMaker |
| Multi-pipeline orchestration | Airflow, Argo Workflows |
| Interactive data exploration | Notebooks, Spark Shell |

## Architecture Decisions

### Why Sequential Execution?

1. **Simplicity**: No complexity of dependency resolution or parallel scheduling
2. **Debuggability**: Easy to understand execution order and trace failures
3. **Predictability**: Same config always produces same execution order
4. **Resource efficiency**: No need for sophisticated resource allocation

### Why HOCON Configuration?

1. **Human-readable**: Easier than JSON, more powerful than YAML
2. **Type-safe**: PureConfig provides compile-time validation
3. **Composable**: Includes and substitutions for environment management
4. **Familiar**: Standard in Scala/Akka ecosystem

### Why Reflection-Based Instantiation?

1. **Decoupling**: Components don't depend on each other at compile time
2. **Pluggability**: Add new components without modifying framework
3. **Configuration-driven**: Component selection via config, not code

## Future Roadmap

The following features are planned for future versions:

### v2.0 (Planned)

- **Schema contracts**: Optional input/output schema validation between components
- **Checkpointing**: Resume pipelines from last successful component
- **Parallel execution**: DAG-based component execution (opt-in)

### Under Consideration

- Structured Streaming support
- Built-in data quality hooks
- Spark UI integration
- Circuit breaker patterns

## Comparison with Alternatives

| Feature | This Framework | Airflow | Prefect | Custom Spark |
|---------|---------------|---------|---------|--------------|
| **Execution** | Sequential | DAG | DAG | Any |
| **Config format** | HOCON | Python | Python | Code |
| **Scheduling** | External | Built-in | Built-in | External |
| **Learning curve** | Low | Medium | Medium | Low |
| **Spark integration** | Native | Operator | Task | Native |
| **Observability** | Built-in | Built-in | Built-in | DIY |

## Summary

Spark Pipeline Framework is the right choice when you need:

- Simple, sequential batch pipelines
- Configuration-driven architecture
- Production-ready observability
- Minimal operational complexity

It is **not** the right choice when you need:

- Complex DAG workflows
- Real-time streaming
- Built-in scheduling
- Schema validation between components

The framework embraces the Unix philosophy: **do one thing well**. For sequential Spark batch pipelines with configuration-driven flexibility, it provides a clean, production-ready solution.
