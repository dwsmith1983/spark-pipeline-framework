package io.github.dwsmith1983.spark.pipeline.audit

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/** Tests for AuditEventSerializer. */
class AuditEventSerializerSpec extends AnyFunSpec with Matchers {

  val testContext: SystemContext = SystemContext(
    hostname = "test-host",
    jvmVersion = "17",
    scalaVersion = "2.13.12",
    sparkVersion = Some("3.5.0"),
    applicationId = Some("app-123"),
    executorId = Some("driver"),
    environment = Map("JAVA_HOME" -> "/usr/lib/jvm")
  )

  val testSparkContext: SparkExecutionContext = SparkExecutionContext(
    applicationId = "app-123",
    applicationName = Some("Test App"),
    master = Some("local[*]"),
    sparkVersion = "3.5.0",
    sparkProperties = Map("spark.executor.memory" -> "4g")
  )

  describe("AuditEventSerializer.toJson") {

    describe("PipelineStartEvent") {

      it("should serialize all fields") {
        val event = PipelineStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test Pipeline",
          componentCount = 2,
          failFast = true,
          systemContext = testContext,
          sparkContext = Some(testSparkContext)
        )

        val json = AuditEventSerializer.toJson(event)

        json should include(""""event_id":"event-123"""")
        json should include(""""run_id":"run-456"""")
        json should include(""""event_type":"pipeline_start"""")
        json should include(""""pipeline_name":"Test Pipeline"""")
        json should include(""""component_count":2""")
        json should include(""""fail_fast":true""")
        json should include(""""hostname":"test-host"""")
        json should include(""""application_id":"app-123"""")
      }

      it("should serialize without Spark context") {
        val event = PipelineStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          componentCount = 1,
          failFast = false,
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        json should include(""""spark_context":null""")
      }
    }

    describe("PipelineEndEvent") {

      it("should serialize success event") {
        val event = PipelineEndEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          durationMs = 1000L,
          status = "success",
          componentsCompleted = 2,
          componentsFailed = 0,
          errorType = None,
          errorMessage = None,
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)

        json should include(""""event_type":"pipeline_end"""")
        json should include(""""status":"success"""")
        json should include(""""duration_ms":1000""")
        json should include(""""components_completed":2""")
        json should include(""""components_failed":0""")
        json should not include "error_type"
      }

      it("should serialize failure event with error info") {
        val event = PipelineEndEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          durationMs = 500L,
          status = "failure",
          componentsCompleted = 1,
          componentsFailed = 1,
          errorType = Some("RuntimeException"),
          errorMessage = Some("Something went wrong"),
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)

        json should include(""""status":"failure"""")
        json should include(""""error_type":"RuntimeException"""")
        json should include(""""error_message":"Something went wrong"""")
      }
    }

    describe("ComponentStartEvent") {

      it("should serialize component config") {
        val event = ComponentStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          componentName = "Component1",
          componentType = "com.example.Component1",
          componentIndex = 1,
          totalComponents = 3,
          componentConfig = Map("key" -> "value", "num" -> "42"),
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)

        json should include(""""event_type":"component_start"""")
        json should include(""""component_name":"Component1"""")
        json should include(""""component_type":"com.example.Component1"""")
        json should include(""""component_index":1""")
        json should include(""""total_components":3""")
        json should include(""""key":"value"""")
      }
    }

    describe("ComponentEndEvent") {

      it("should serialize duration and status") {
        val event = ComponentEndEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          componentName = "Component1",
          componentType = "com.example.Component1",
          componentIndex = 1,
          totalComponents = 3,
          durationMs = 500L,
          status = "success",
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)

        json should include(""""event_type":"component_end"""")
        json should include(""""duration_ms":500""")
        json should include(""""status":"success"""")
      }
    }

    describe("ComponentFailureEvent") {

      it("should serialize error details") {
        val event = ComponentFailureEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          componentName = "Component1",
          componentType = "com.example.Component1",
          componentIndex = 1,
          durationMs = 100L,
          errorType = "RuntimeException",
          errorMessage = "Test error",
          stackTrace = Some("at com.example..."),
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)

        json should include(""""event_type":"component_failure"""")
        json should include(""""error_type":"RuntimeException"""")
        json should include(""""error_message":"Test error"""")
        json should include(""""stack_trace":"at com.example..."""")
      }

      it("should omit stack trace when None") {
        val event = ComponentFailureEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          componentName = "Component1",
          componentType = "com.example.Component1",
          componentIndex = 1,
          durationMs = 100L,
          errorType = "RuntimeException",
          errorMessage = "Test error",
          stackTrace = None,
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        json should not include "stack_trace"
      }
    }

    describe("JSON escaping") {

      it("should escape quotes in strings") {
        val event = PipelineStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = """Test "Pipeline"""",
          componentCount = 1,
          failFast = true,
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        json should include(""""pipeline_name":"Test \"Pipeline\""""")
      }

      it("should escape newlines in strings") {
        val event = PipelineStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test\nPipeline",
          componentCount = 1,
          failFast = true,
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        // \n in the input becomes \\n in JSON (escaped newline)
        json should include("\"pipeline_name\":\"Test\\nPipeline\"")
      }

      it("should escape backslashes in strings") {
        val event = PipelineStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = """Test\Pipeline""",
          componentCount = 1,
          failFast = true,
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        json should include(""""pipeline_name":"Test\\Pipeline"""")
      }
    }

    describe("timestamp format") {

      it("should format timestamp as ISO-8601") {
        val event = PipelineStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-06-15T14:30:00Z"),
          pipelineName = "Test",
          componentCount = 1,
          failFast = true,
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        json should include(""""timestamp":"2024-06-15T14:30:00Z"""")
      }
    }

    describe("edge cases") {

      it("should handle empty strings in fields") {
        val event = PipelineStartEvent(
          eventId = "",
          runId = "",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "",
          componentCount = 0,
          failFast = true,
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        json should include(""""event_id":""""")
        json should include(""""run_id":""""")
        json should include(""""pipeline_name":""""")
      }

      it("should handle empty component config map") {
        val event = ComponentStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          componentName = "Comp1",
          componentType = "com.example.Comp1",
          componentIndex = 1,
          totalComponents = 1,
          componentConfig = Map.empty,
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        json should include(""""component_config":{}""")
      }

      it("should handle empty environment map") {
        val emptyEnvContext = testContext.copy(environment = Map.empty)
        val event = PipelineStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          componentCount = 1,
          failFast = true,
          systemContext = emptyEnvContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        json should include(""""environment":{}""")
      }

      it("should handle unicode characters") {
        val event = PipelineStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test 日本語 émoji 🚀",
          componentCount = 1,
          failFast = true,
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        json should include("日本語")
        json should include("émoji")
        json should include("🚀")
      }

      it("should handle very long pipeline names") {
        val longName = "x" * 1000
        val event = PipelineStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = longName,
          componentCount = 1,
          failFast = true,
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        json should include(longName)
      }

      it("should handle large component index values") {
        val event = ComponentEndEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          componentName = "Comp",
          componentType = "com.example.Comp",
          componentIndex = Int.MaxValue,
          totalComponents = Int.MaxValue,
          durationMs = Long.MaxValue,
          status = "success",
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        json should include(s""""component_index":${Int.MaxValue}""")
        json should include(s""""duration_ms":${Long.MaxValue}""")
      }

      it("should handle stack trace with special characters") {
        val stackTrace =
          """java.lang.RuntimeException: Test "error"
            |	at com.example.Test.method(Test.scala:42)
            |	at com.example.Test$$.run(Test.scala:10)
            |Caused by: java.io.IOException: File not found
            |	at java.io.FileInputStream.<init>(FileInputStream.java:123)""".stripMargin

        val event = ComponentFailureEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          componentName = "Comp",
          componentType = "com.example.Comp",
          componentIndex = 1,
          durationMs = 100L,
          errorType = "RuntimeException",
          errorMessage = "Test error",
          stackTrace = Some(stackTrace),
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        // Should properly escape quotes in stack trace
        json should include("\\\"error\\\"")
        // Should properly escape newlines
        json should include("\\n\\tat")
      }

      it("should handle config values with JSON-like content") {
        val event = ComponentStartEvent(
          eventId = "event-123",
          runId = "run-456",
          timestamp = Instant.parse("2024-01-01T00:00:00Z"),
          pipelineName = "Test",
          componentName = "Comp1",
          componentType = "com.example.Comp1",
          componentIndex = 1,
          totalComponents = 1,
          componentConfig = Map("json_value" -> """{"nested": "value", "array": [1,2,3]}"""),
          systemContext = testContext,
          sparkContext = None
        )

        val json = AuditEventSerializer.toJson(event)
        // The JSON-like value should be escaped
        json should include("\\\"nested\\\"")
        json should include("\\\"value\\\"")
      }
    }
  }
}
