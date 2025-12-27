package io.github.dwsmith1983.spark.pipeline.audit

import java.time.Instant

/**
 * Context about the execution environment.
 *
 * Always available without Spark dependency.
 *
 * @param hostname The hostname of the machine running the pipeline
 * @param jvmVersion The JVM version (java.version system property)
 * @param scalaVersion The Scala version string
 * @param sparkVersion Spark version if available
 * @param applicationId Spark application ID if available
 * @param executorId Spark executor ID if available ("driver" for driver)
 * @param environment Filtered environment variables
 */
case class SystemContext(
  hostname: String,
  jvmVersion: String,
  scalaVersion: String,
  sparkVersion: Option[String] = None,
  applicationId: Option[String] = None,
  executorId: Option[String] = None,
  environment: Map[String, String] = Map.empty)

/**
 * Spark-specific execution context.
 *
 * Available when running with an active SparkSession.
 *
 * @param applicationId The Spark application ID
 * @param applicationName The Spark application name
 * @param master The Spark master URL
 * @param sparkVersion The Spark version
 * @param sparkProperties Filtered Spark configuration properties
 */
case class SparkExecutionContext(
  applicationId: String,
  applicationName: Option[String] = None,
  master: Option[String] = None,
  sparkVersion: String,
  sparkProperties: Map[String, String] = Map.empty)

/**
 * Base trait for all audit events.
 *
 * Each event captures a point in the pipeline lifecycle with full context
 * for after-the-fact analysis and debugging.
 */
sealed trait AuditEvent {

  /** Unique identifier for this event */
  def eventId: String

  /** Correlation ID for the pipeline run (same across all events in a run) */
  def runId: String

  /** Event type for filtering/querying (e.g., "pipeline_start", "component_end") */
  def eventType: String

  /** When this event occurred */
  def timestamp: Instant

  /** System context (always available) */
  def systemContext: SystemContext

  /** Spark context (available when SparkSession is active) */
  def sparkContext: Option[SparkExecutionContext]
}

/** Emitted when pipeline execution begins. */
final case class PipelineStartEvent(
  eventId: String,
  runId: String,
  timestamp: Instant,
  pipelineName: String,
  componentCount: Int,
  failFast: Boolean,
  systemContext: SystemContext,
  sparkContext: Option[SparkExecutionContext])
  extends AuditEvent {
  override val eventType: String = "pipeline_start"
}

/** Emitted when pipeline execution completes. */
final case class PipelineEndEvent(
  eventId: String,
  runId: String,
  timestamp: Instant,
  pipelineName: String,
  durationMs: Long,
  status: String,
  componentsCompleted: Int,
  componentsFailed: Int,
  errorType: Option[String],
  errorMessage: Option[String],
  systemContext: SystemContext,
  sparkContext: Option[SparkExecutionContext])
  extends AuditEvent {
  override val eventType: String = "pipeline_end"
}

/** Emitted when a component starts execution. */
final case class ComponentStartEvent(
  eventId: String,
  runId: String,
  timestamp: Instant,
  pipelineName: String,
  componentName: String,
  componentType: String,
  componentIndex: Int,
  totalComponents: Int,
  componentConfig: Map[String, String],
  systemContext: SystemContext,
  sparkContext: Option[SparkExecutionContext])
  extends AuditEvent {
  override val eventType: String = "component_start"
}

/** Emitted when a component completes successfully. */
final case class ComponentEndEvent(
  eventId: String,
  runId: String,
  timestamp: Instant,
  pipelineName: String,
  componentName: String,
  componentType: String,
  componentIndex: Int,
  totalComponents: Int,
  durationMs: Long,
  status: String,
  systemContext: SystemContext,
  sparkContext: Option[SparkExecutionContext])
  extends AuditEvent {
  override val eventType: String = "component_end"
}

/** Emitted when a component fails. */
final case class ComponentFailureEvent(
  eventId: String,
  runId: String,
  timestamp: Instant,
  pipelineName: String,
  componentName: String,
  componentType: String,
  componentIndex: Int,
  durationMs: Long,
  errorType: String,
  errorMessage: String,
  stackTrace: Option[String],
  systemContext: SystemContext,
  sparkContext: Option[SparkExecutionContext])
  extends AuditEvent {
  override val eventType: String = "component_failure"
}
