package io.github.dwsmith1983.spark.pipeline.audit

import com.typesafe.config.ConfigFactory
import io.github.dwsmith1983.spark.pipeline.config._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import scala.collection.mutable.ListBuffer

/** In-memory sink for testing */
class TestAuditSink extends AuditSink {
  val events: ListBuffer[AuditEvent] = ListBuffer.empty
  var flushed: Boolean               = false
  var closed: Boolean                = false

  override def write(event: AuditEvent): Unit = events += event
  override def flush(): Unit                  = flushed = true
  override def close(): Unit                  = closed = true

  def clear(): Unit = {
    events.clear()
    flushed = false
    closed = false
  }
}

/** Tests for AuditHooks. */
class AuditHooksSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  var sink: TestAuditSink = _
  var hooks: AuditHooks   = _

  val testPipelineConfig: PipelineConfig = PipelineConfig(
    pipelineName = "Test Pipeline",
    pipelineComponents = List(
      ComponentConfig(
        instanceType = "com.example.Component1",
        instanceName = "Component1",
        instanceConfig = ConfigFactory.parseString("input.path = /data/input, password = secret123")
      ),
      ComponentConfig(
        instanceType = "com.example.Component2",
        instanceName = "Component2",
        instanceConfig = ConfigFactory.empty()
      )
    )
  )

  val testComponentConfig: ComponentConfig = testPipelineConfig.pipelineComponents.head

  override def beforeEach(): Unit = {
    sink = new TestAuditSink()
    hooks = new AuditHooks(sink, runId = "test-run-123")
  }

  describe("AuditHooks") {

    describe("happy path") {

      it("should write pipeline_start event on beforePipeline") {
        hooks.beforePipeline(testPipelineConfig)

        (sink.events should have).length(1)
        val event = sink.events.head.asInstanceOf[PipelineStartEvent]
        event.eventType shouldBe "pipeline_start"
        event.runId shouldBe "test-run-123"
        event.pipelineName shouldBe "Test Pipeline"
        event.componentCount shouldBe 2
        event.failFast shouldBe true
      }

      it("should write pipeline_end event with success status") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 2))

        (sink.events should have).length(2)
        val event = sink.events.last.asInstanceOf[PipelineEndEvent]
        event.eventType shouldBe "pipeline_end"
        event.status shouldBe "success"
        event.durationMs shouldBe 100L
        event.componentsCompleted shouldBe 2
        event.componentsFailed shouldBe 0
        event.errorType shouldBe None
        event.errorMessage shouldBe None
      }

      it("should write component_start event with config snapshot") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)

        (sink.events should have).length(2)
        val event = sink.events.last.asInstanceOf[ComponentStartEvent]
        event.eventType shouldBe "component_start"
        event.componentName shouldBe "Component1"
        event.componentType shouldBe "com.example.Component1"
        event.componentIndex shouldBe 1
        event.totalComponents shouldBe 2
        (event.componentConfig should contain).key("input.path")
      }

      it("should write component_end event with duration") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.afterComponent(testComponentConfig, 0, 2, 50L)

        (sink.events should have).length(3)
        val event = sink.events.last.asInstanceOf[ComponentEndEvent]
        event.eventType shouldBe "component_end"
        event.componentName shouldBe "Component1"
        event.durationMs shouldBe 50L
        event.status shouldBe "success"
      }

      it("should include system context in all events") {
        hooks.beforePipeline(testPipelineConfig)

        val event = sink.events.head
        event.systemContext.hostname should not be empty
        event.systemContext.jvmVersion should not be empty
        event.systemContext.scalaVersion should not be empty
      }

      it("should use provided runId for all events") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.afterComponent(testComponentConfig, 0, 2, 50L)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 2))

        sink.events.foreach(event => event.runId shouldBe "test-run-123")
      }

      it("should auto-generate runId if not provided") {
        val autoHooks = new AuditHooks(sink)
        autoHooks.beforePipeline(testPipelineConfig)

        val event = sink.events.head
        event.runId should not be empty
        (event.runId should have).length(36) // UUID format
      }

      it("should call sink.flush() after pipeline completion") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 2))

        sink.flushed shouldBe true
      }

      it("should handle multiple components in sequence") {
        val comp1 = testPipelineConfig.pipelineComponents.head
        val comp2 = testPipelineConfig.pipelineComponents(1)

        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(comp1, 0, 2)
        hooks.afterComponent(comp1, 0, 2, 50L)
        hooks.beforeComponent(comp2, 1, 2)
        hooks.afterComponent(comp2, 1, 2, 75L)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(125L, 2))

        (sink.events should have).length(6)
        sink.events.count(_.isInstanceOf[ComponentStartEvent]) shouldBe 2
        sink.events.count(_.isInstanceOf[ComponentEndEvent]) shouldBe 2
      }

      it("should generate unique eventIds for each event") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.afterComponent(testComponentConfig, 0, 2, 50L)

        val eventIds = sink.events.map(_.eventId)
        (eventIds.distinct should have).length(eventIds.length)
      }
    }

    describe("failure handling") {

      it("should write component_failure event on onComponentFailure") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.onComponentFailure(testComponentConfig, 0, new RuntimeException("Test error"))

        (sink.events should have).length(3)
        val event = sink.events.last.asInstanceOf[ComponentFailureEvent]
        event.eventType shouldBe "component_failure"
        event.errorType shouldBe "RuntimeException"
        event.errorMessage shouldBe "Test error"
      }

      it("should write pipeline_end with failure status") {
        val error = new RuntimeException("Pipeline failed")
        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Failure(error, Some(testComponentConfig), 1))

        val event = sink.events.last.asInstanceOf[PipelineEndEvent]
        event.status shouldBe "failure"
        event.errorType shouldBe Some("RuntimeException")
        event.errorMessage shouldBe Some("Pipeline failed")
      }

      it("should write partial_success status when failFast=false") {
        hooks.beforePipeline(testPipelineConfig.copy(failFast = false))
        hooks.afterPipeline(
          testPipelineConfig,
          PipelineResult.PartialSuccess(200L, 1, 1, List((testComponentConfig, new RuntimeException("Error"))))
        )

        val event = sink.events.last.asInstanceOf[PipelineEndEvent]
        event.status shouldBe "partial_success"
        event.componentsCompleted shouldBe 1
        event.componentsFailed shouldBe 1
      }

      it("should capture error type and message") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.onComponentFailure(testComponentConfig, 0, new IllegalArgumentException("Bad argument"))

        val event = sink.events.last.asInstanceOf[ComponentFailureEvent]
        event.errorType shouldBe "IllegalArgumentException"
        event.errorMessage shouldBe "Bad argument"
      }

      it("should include stack trace when enabled") {
        val hooksWithStack = new AuditHooks(sink, includeStackTrace = true)
        hooksWithStack.beforePipeline(testPipelineConfig)
        hooksWithStack.beforeComponent(testComponentConfig, 0, 2)
        hooksWithStack.onComponentFailure(testComponentConfig, 0, new RuntimeException("Test"))

        val event = sink.events.last.asInstanceOf[ComponentFailureEvent]
        event.stackTrace shouldBe defined
        event.stackTrace.get should include("RuntimeException")
      }

      it("should omit stack trace when disabled") {
        val hooksNoStack = new AuditHooks(sink, includeStackTrace = false)
        hooksNoStack.beforePipeline(testPipelineConfig)
        hooksNoStack.beforeComponent(testComponentConfig, 0, 2)
        hooksNoStack.onComponentFailure(testComponentConfig, 0, new RuntimeException("Test"))

        val event = sink.events.last.asInstanceOf[ComponentFailureEvent]
        event.stackTrace shouldBe None
      }

      it("should handle null error message gracefully") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.onComponentFailure(testComponentConfig, 0, new RuntimeException(null: String))

        val event = sink.events.last.asInstanceOf[ComponentFailureEvent]
        event.errorMessage shouldBe "No message"
      }

      it("should calculate component duration even on failure") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        Thread.sleep(10)
        hooks.onComponentFailure(testComponentConfig, 0, new RuntimeException("Test"))

        val event = sink.events.last.asInstanceOf[ComponentFailureEvent]
        event.durationMs should be >= 10L
      }
    }

    describe("missing state scenarios") {

      it("should use 'unknown' pipeline name when state missing") {
        // Call beforeComponent without beforePipeline
        hooks.beforeComponent(testComponentConfig, 0, 2)

        val event = sink.events.head.asInstanceOf[ComponentStartEvent]
        event.pipelineName shouldBe "unknown"
      }

      it("should write events even without prior beforePipeline") {
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.afterComponent(testComponentConfig, 0, 2, 50L)

        (sink.events should have).length(2)
      }

      it("should handle component failure without prior beforeComponent") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.onComponentFailure(testComponentConfig, 0, new RuntimeException("Test"))

        (sink.events should have).length(2)
        val event = sink.events.last.asInstanceOf[ComponentFailureEvent]
        event.durationMs should be >= 0L
      }
    }

    describe("state management") {

      it("should reset state between pipeline runs") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 2))

        val firstRunId = sink.events.head.runId

        sink.clear()
        val newHooks = new AuditHooks(sink)
        newHooks.beforePipeline(testPipelineConfig)

        sink.events.head.runId should not be firstRunId
      }

      it("should track multiple components correctly") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testPipelineConfig.pipelineComponents(0), 0, 2)
        hooks.beforeComponent(testPipelineConfig.pipelineComponents(1), 1, 2)

        val starts = sink.events.collect { case e: ComponentStartEvent => e }
        (starts should have).length(2)
        starts(0).componentIndex shouldBe 1
        starts(1).componentIndex shouldBe 2
      }
    }

    describe("filter integration") {

      it("should filter sensitive config keys") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)

        val event = sink.events.last.asInstanceOf[ComponentStartEvent]
        event.componentConfig("password") shouldBe "***REDACTED***"
        event.componentConfig("input.path") should include("/data/input")
      }

      it("should use custom config filter") {
        val customFilter = ConfigFilter.withPatterns(Set("input"))
        val customHooks  = new AuditHooks(sink, configFilter = customFilter)

        customHooks.beforePipeline(testPipelineConfig)
        customHooks.beforeComponent(testComponentConfig, 0, 2)

        val event = sink.events.last.asInstanceOf[ComponentStartEvent]
        event.componentConfig("input.path") shouldBe "***REDACTED***"
      }

      it("should filter environment variables") {
        hooks.beforePipeline(testPipelineConfig)

        val event = sink.events.head
        // Default filter only includes safe variables
        event.systemContext.environment.keys.foreach(key => EnvFilter.DefaultAllowKeys should contain(key))
      }

      it("should use custom env filter") {
        val customFilter = EnvFilter.empty
        val customHooks  = new AuditHooks(sink, envFilter = customFilter)

        customHooks.beforePipeline(testPipelineConfig)

        val event = sink.events.head
        event.systemContext.environment shouldBe empty
      }
    }

    describe("sink integration") {

      it("should write events through provided sink") {
        hooks.beforePipeline(testPipelineConfig)
        (sink.events should have).length(1)
      }

      it("should work with NoOp sink") {
        val noOpHooks = new AuditHooks(AuditSink.NoOp)
        noOpHooks.beforePipeline(testPipelineConfig)
        noOpHooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 2))
        // Should not throw
      }
    }

    describe("edge cases") {

      it("should handle empty pipeline (no components)") {
        val emptyConfig = PipelineConfig("Empty", List.empty)
        hooks.beforePipeline(emptyConfig)
        hooks.afterPipeline(emptyConfig, PipelineResult.Success(10L, 0))

        val start = sink.events.head.asInstanceOf[PipelineStartEvent]
        start.componentCount shouldBe 0

        val end = sink.events.last.asInstanceOf[PipelineEndEvent]
        end.componentsCompleted shouldBe 0
      }

      it("should handle special characters in names") {
        val specialConfig = PipelineConfig(
          "Test \"Pipeline\" with 'quotes'",
          List(
            ComponentConfig(
              "com.example.Test",
              "Component\nwith\nnewlines",
              ConfigFactory.empty()
            )
          )
        )

        hooks.beforePipeline(specialConfig)
        hooks.beforeComponent(specialConfig.pipelineComponents.head, 0, 1)

        // Should not throw and events should be written
        (sink.events should have).length(2)
      }
    }
  }

  describe("AuditHooks factory methods") {

    it("should create hooks with toFile") {
      val hooks = AuditHooks.toFile("/tmp/test-audit.jsonl")
      hooks.sink shouldBe a[FileAuditSink]
    }

    it("should create hooks with toFile and runId") {
      val hooks = AuditHooks.toFile("/tmp/test-audit.jsonl", "custom-run-id")
      hooks.runId shouldBe "custom-run-id"
    }

    it("should create hooks with apply(sink)") {
      val hooks = AuditHooks(sink)
      hooks.sink shouldBe sink
    }
  }
}
