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

- **Batch ETL pipelines** with clear input → transform → output stages
- **Streaming pipelines** with sequential data transformations
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

### Streaming Support

The framework supports **both batch and streaming pipelines** via Spark Structured Streaming integration:

- Streaming sources: Kafka, Kinesis, EventHubs, File, Delta, Iceberg
- Streaming sinks: Kafka, Kinesis, EventHubs, File, Delta, Iceberg, Console
- Sequential processing of streaming DataFrames
- Configuration-driven streaming component definition

See [Streaming](./streaming.md) for details.

**For complex stream processing**, consider:
- Apache Flink (native streaming engine)
- Apache Kafka Streams (Kafka-native processing)

### Optional Schema Contracts

Components can optionally declare input/output schemas via the `SchemaContract` trait. When enabled:

- Runtime validation checks schema compatibility between adjacent components
- Clear error messages when schemas don't match
- Validation is disabled by default for backward compatibility

See [Schema Contracts](./schema-contracts.md) for details.

**For advanced data quality validation**, combine with:
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
| Batch ETL pipelines | Sequential stages, clear flow |
| Streaming ETL pipelines | Structured Streaming with ordered transforms |
| Data migration jobs | Ordered steps, config-driven |
| Batch report generation | Predictable execution order |
| Feature engineering | Transform chains without branches |
| Data quality pipelines | Sequential validation checks |

### Not a Good Fit

| Scenario | Better Alternative |
|----------|-------------------|
| Complex DAG workflows | Airflow, Prefect, Dagster |
| Complex event processing | Apache Flink |
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

The framework's core features are now complete. Future development will focus on refinements and cloud-native enhancements:

### Completed Features

- **v1.1.0**: Configuration validation, Secrets management, Streaming core
- **v1.2.0**: Schema contracts, Checkpointing, Retry logic, Data quality hooks
- **v1.3.0**: Complete streaming sources/sinks (Kafka, Kinesis, EventHubs, File, Delta, Iceberg)

### Under Consideration

- Additional streaming source/sink connectors
- Enhanced observability integrations
- Cloud-specific optimizations

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

- Simple, sequential batch or streaming pipelines
- Configuration-driven architecture
- Production-ready observability
- Optional schema contracts between components
- Structured Streaming integration
- Minimal operational complexity

It is **not** the right choice when you need:

- Complex DAG workflows with parallel execution
- Complex event processing or stateful stream operations
- Built-in scheduling and orchestration

The framework embraces the Unix philosophy: **do one thing well**. For sequential Spark pipelines (batch or streaming) with configuration-driven flexibility, it provides a clean, production-ready solution.
