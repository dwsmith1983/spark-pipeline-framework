package io.github.dwsmith1983.spark.pipeline

/**
 * Audit trail for pipeline execution.
 *
 * Provides persistent, structured audit events for after-the-fact analysis
 * of pipeline executions, with pluggable storage backends and security filtering.
 *
 * == Quick Start ==
 *
 * {{{
 * import io.github.dwsmith1983.spark.pipeline.audit._
 *
 * // Create audit hooks with file output
 * val hooks = AuditHooks.toFile("/var/log/pipeline-audit.jsonl")
 *
 * // Run pipeline with audit
 * SimplePipelineRunner.run(config, hooks)
 * }}}
 *
 * == Components ==
 *
 * '''AuditHooks''': PipelineHooks implementation that writes audit events
 * at each lifecycle point (pipeline start/end, component start/end/failure).
 *
 * '''AuditSink''': Pluggable interface for audit event destinations.
 * Built-in implementations:
 *   - `AuditSink.file(path)` - JSON lines file
 *   - `AuditSink.compose(sink1, sink2)` - Multiple sinks
 *   - `AuditSink.NoOp` - Discard events
 *
 * '''AuditEvent''': Sealed trait with event types:
 *   - `PipelineStartEvent` / `PipelineEndEvent`
 *   - `ComponentStartEvent` / `ComponentEndEvent` / `ComponentFailureEvent`
 *
 * '''ConfigFilter''' / '''EnvFilter''': Security filters for redacting
 * sensitive values in configuration and environment variables.
 *
 * '''AuditContextProvider''': Extracts system/Spark context for events.
 * Use `SparkAuditContextProvider` from runtime module for Spark enrichment.
 *
 * == Extending with Custom Sinks ==
 *
 * {{{
 * class WebhookAuditSink(url: String) extends AuditSink {
 *   override def write(event: AuditEvent): Unit = {
 *     val json = AuditEventSerializer.toJson(event)
 *     // POST to webhook
 *   }
 *   override def flush(): Unit = ()
 *   override def close(): Unit = ()
 * }
 *
 * val hooks = AuditHooks(new WebhookAuditSink("https://audit.example.com"))
 * }}}
 */
package object audit {

  /** Default configuration filter for redacting sensitive values. */
  val DefaultConfigFilter: ConfigFilter = ConfigFilter.default

  /** Default environment filter for safe variable exposure. */
  val DefaultEnvFilter: EnvFilter = EnvFilter.default
}
