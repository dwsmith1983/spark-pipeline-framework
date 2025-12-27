package io.github.dwsmith1983.spark.pipeline.audit

import java.time.format.DateTimeFormatter

/**
 * JSON serialization for audit events.
 *
 * Uses manual serialization to avoid external dependencies and
 * ensure compatibility across Scala 2.12/2.13.
 */
object AuditEventSerializer {

  private val timestampFormatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

  /**
   * Serialize an audit event to JSON.
   *
   * @param event The audit event to serialize
   * @return JSON string representation
   */
  def toJson(event: AuditEvent): String = event match {
    case e: PipelineStartEvent    => pipelineStartToJson(e)
    case e: PipelineEndEvent      => pipelineEndToJson(e)
    case e: ComponentStartEvent   => componentStartToJson(e)
    case e: ComponentEndEvent     => componentEndToJson(e)
    case e: ComponentFailureEvent => componentFailureToJson(e)
  }

  private def escapeJson(s: String): String =
    if (s == null) {
      ""
    } else {
      s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("\b", "\\b")
        .replace("\f", "\\f")
    }

  private def mapToJson(m: Map[String, String]): String =
    if (m.isEmpty) {
      "{}"
    } else {
      m.map {
        case (k, v) =>
          s""""${escapeJson(k)}":"${escapeJson(v)}""""
      }.mkString("{", ",", "}")
    }

  private def systemContextToJson(ctx: SystemContext): String = {
    val parts = List(
      Some(s""""hostname":"${escapeJson(ctx.hostname)}""""),
      Some(s""""jvm_version":"${escapeJson(ctx.jvmVersion)}""""),
      Some(s""""scala_version":"${escapeJson(ctx.scalaVersion)}""""),
      ctx.sparkVersion.map(v => s""""spark_version":"${escapeJson(v)}""""),
      ctx.applicationId.map(v => s""""application_id":"${escapeJson(v)}""""),
      ctx.executorId.map(v => s""""executor_id":"${escapeJson(v)}""""),
      Some(s""""environment":${mapToJson(ctx.environment)}""")
    ).flatten
    "{" + parts.mkString(",") + "}"
  }

  private def sparkContextToJson(ctx: Option[SparkExecutionContext]): String =
    ctx match {
      case None => "null"
      case Some(sc) =>
        val parts = List(
          Some(s""""application_id":"${escapeJson(sc.applicationId)}""""),
          sc.applicationName.map(v => s""""application_name":"${escapeJson(v)}""""),
          sc.master.map(v => s""""master":"${escapeJson(v)}""""),
          Some(s""""spark_version":"${escapeJson(sc.sparkVersion)}""""),
          Some(s""""spark_properties":${mapToJson(sc.sparkProperties)}""")
        ).flatten
        "{" + parts.mkString(",") + "}"
    }

  private def pipelineStartToJson(e: PipelineStartEvent): String = {
    val sb = new StringBuilder
    sb.append(s"""{"event_id":"${escapeJson(e.eventId)}",""")
    sb.append(s""""run_id":"${escapeJson(e.runId)}",""")
    sb.append(s""""event_type":"${e.eventType}",""")
    sb.append(s""""timestamp":"${timestampFormatter.format(e.timestamp)}",""")
    sb.append(s""""pipeline_name":"${escapeJson(e.pipelineName)}",""")
    sb.append(s""""component_count":${e.componentCount},""")
    sb.append(s""""fail_fast":${e.failFast},""")
    sb.append(s""""system_context":${systemContextToJson(e.systemContext)},""")
    sb.append(s""""spark_context":${sparkContextToJson(e.sparkContext)}}""")
    sb.toString()
  }

  private def pipelineEndToJson(e: PipelineEndEvent): String = {
    val sb = new StringBuilder
    sb.append(s"""{"event_id":"${escapeJson(e.eventId)}",""")
    sb.append(s""""run_id":"${escapeJson(e.runId)}",""")
    sb.append(s""""event_type":"${e.eventType}",""")
    sb.append(s""""timestamp":"${timestampFormatter.format(e.timestamp)}",""")
    sb.append(s""""pipeline_name":"${escapeJson(e.pipelineName)}",""")
    sb.append(s""""duration_ms":${e.durationMs},""")
    sb.append(s""""status":"${e.status}",""")
    sb.append(s""""components_completed":${e.componentsCompleted},""")
    sb.append(s""""components_failed":${e.componentsFailed}""")
    e.errorType.foreach(t => sb.append(s""","error_type":"${escapeJson(t)}""""))
    e.errorMessage.foreach(m => sb.append(s""","error_message":"${escapeJson(m)}""""))
    sb.append(s""","system_context":${systemContextToJson(e.systemContext)},""")
    sb.append(s""""spark_context":${sparkContextToJson(e.sparkContext)}}""")
    sb.toString()
  }

  private def componentStartToJson(e: ComponentStartEvent): String = {
    val sb = new StringBuilder
    sb.append(s"""{"event_id":"${escapeJson(e.eventId)}",""")
    sb.append(s""""run_id":"${escapeJson(e.runId)}",""")
    sb.append(s""""event_type":"${e.eventType}",""")
    sb.append(s""""timestamp":"${timestampFormatter.format(e.timestamp)}",""")
    sb.append(s""""pipeline_name":"${escapeJson(e.pipelineName)}",""")
    sb.append(s""""component_name":"${escapeJson(e.componentName)}",""")
    sb.append(s""""component_type":"${escapeJson(e.componentType)}",""")
    sb.append(s""""component_index":${e.componentIndex},""")
    sb.append(s""""total_components":${e.totalComponents},""")
    sb.append(s""""component_config":${mapToJson(e.componentConfig)},""")
    sb.append(s""""system_context":${systemContextToJson(e.systemContext)},""")
    sb.append(s""""spark_context":${sparkContextToJson(e.sparkContext)}}""")
    sb.toString()
  }

  private def componentEndToJson(e: ComponentEndEvent): String = {
    val sb = new StringBuilder
    sb.append(s"""{"event_id":"${escapeJson(e.eventId)}",""")
    sb.append(s""""run_id":"${escapeJson(e.runId)}",""")
    sb.append(s""""event_type":"${e.eventType}",""")
    sb.append(s""""timestamp":"${timestampFormatter.format(e.timestamp)}",""")
    sb.append(s""""pipeline_name":"${escapeJson(e.pipelineName)}",""")
    sb.append(s""""component_name":"${escapeJson(e.componentName)}",""")
    sb.append(s""""component_type":"${escapeJson(e.componentType)}",""")
    sb.append(s""""component_index":${e.componentIndex},""")
    sb.append(s""""total_components":${e.totalComponents},""")
    sb.append(s""""duration_ms":${e.durationMs},""")
    sb.append(s""""status":"${e.status}",""")
    sb.append(s""""system_context":${systemContextToJson(e.systemContext)},""")
    sb.append(s""""spark_context":${sparkContextToJson(e.sparkContext)}}""")
    sb.toString()
  }

  private def componentFailureToJson(e: ComponentFailureEvent): String = {
    val sb = new StringBuilder
    sb.append(s"""{"event_id":"${escapeJson(e.eventId)}",""")
    sb.append(s""""run_id":"${escapeJson(e.runId)}",""")
    sb.append(s""""event_type":"${e.eventType}",""")
    sb.append(s""""timestamp":"${timestampFormatter.format(e.timestamp)}",""")
    sb.append(s""""pipeline_name":"${escapeJson(e.pipelineName)}",""")
    sb.append(s""""component_name":"${escapeJson(e.componentName)}",""")
    sb.append(s""""component_type":"${escapeJson(e.componentType)}",""")
    sb.append(s""""component_index":${e.componentIndex},""")
    sb.append(s""""duration_ms":${e.durationMs},""")
    sb.append(s""""error_type":"${escapeJson(e.errorType)}",""")
    sb.append(s""""error_message":"${escapeJson(e.errorMessage)}"""")
    e.stackTrace.foreach(st => sb.append(s""","stack_trace":"${escapeJson(st)}""""))
    sb.append(s""","system_context":${systemContextToJson(e.systemContext)},""")
    sb.append(s""""spark_context":${sparkContextToJson(e.sparkContext)}}""")
    sb.toString()
  }
}
