package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

/** Tests for LoggingHooks. */
class LoggingHooksSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  val samplePipelineConfig: PipelineConfig = PipelineConfig(
    pipelineName = "Test Pipeline",
    pipelineComponents = List(
      ComponentConfig(
        instanceType = "com.example.Component1",
        instanceName = "Component1",
        instanceConfig = ConfigFactory.empty()
      ),
      ComponentConfig(
        instanceType = "com.example.Component2",
        instanceName = "Component2",
        instanceConfig = ConfigFactory.empty()
      )
    )
  )

  val sampleComponentConfig: ComponentConfig = ComponentConfig(
    instanceType = "com.example.TestComponent",
    instanceName = "TestComponent",
    instanceConfig = ConfigFactory.empty()
  )

  // Test appender to capture log messages
  private var testAppender: TestLogAppender = _
  private var originalLevel: Level          = _

  override def beforeEach(): Unit = {
    testAppender = new TestLogAppender("TestAppender")
    testAppender.start()

    val ctx: LoggerContext =
      org.apache.logging.log4j.LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config       = ctx.getConfiguration
    val loggerConfig = config.getLoggerConfig(classOf[LoggingHooks].getName)
    originalLevel = loggerConfig.getLevel
    loggerConfig.setLevel(Level.DEBUG)
    loggerConfig.addAppender(testAppender, Level.DEBUG, null)
    ctx.updateLoggers()
  }

  override def afterEach(): Unit = {
    val ctx: LoggerContext =
      org.apache.logging.log4j.LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config       = ctx.getConfiguration
    val loggerConfig = config.getLoggerConfig(classOf[LoggingHooks].getName)
    loggerConfig.removeAppender("TestAppender")
    loggerConfig.setLevel(originalLevel)
    ctx.updateLoggers()
    testAppender.stop()
  }

  describe("LoggingHooks") {

    describe("with structured format") {

      it("should log pipeline_start event as JSON") {
        val hooks = new LoggingHooks(runId = "test-123", useStructuredFormat = true)
        hooks.beforePipeline(samplePipelineConfig)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include(""""event":"pipeline_start"""")
        logs.head should include(""""run_id":"test-123"""")
        logs.head should include(""""pipeline_name":"Test Pipeline"""")
        logs.head should include(""""component_count":2""")
        logs.head should include(""""timestamp":""")
      }

      it("should log pipeline_end event on success as JSON") {
        val hooks = new LoggingHooks(runId = "test-456", useStructuredFormat = true)
        hooks.beforePipeline(samplePipelineConfig)
        testAppender.clear()

        hooks.afterPipeline(samplePipelineConfig, PipelineResult.Success(1500L, 2))

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include(""""event":"pipeline_end"""")
        logs.head should include(""""run_id":"test-456"""")
        logs.head should include(""""duration_ms":1500""")
        logs.head should include(""""status":"success"""")
        logs.head should include(""""components_completed":2""")
      }

      it("should log pipeline_end event on failure as JSON") {
        val hooks = new LoggingHooks(runId = "test-789", useStructuredFormat = true)
        hooks.beforePipeline(samplePipelineConfig)
        testAppender.clear()

        val failure = PipelineResult.Failure(
          new RuntimeException("Test error"),
          Some(sampleComponentConfig),
          1
        )
        hooks.afterPipeline(samplePipelineConfig, failure)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include(""""event":"pipeline_end"""")
        logs.head should include(""""status":"failure"""")
        logs.head should include(""""components_completed":1""")
      }

      it("should log component_start event as JSON") {
        val hooks = new LoggingHooks(runId = "comp-start", useStructuredFormat = true)
        hooks.beforeComponent(sampleComponentConfig, 0, 3)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include(""""event":"component_start"""")
        logs.head should include(""""run_id":"comp-start"""")
        logs.head should include(""""component_name":"TestComponent"""")
        logs.head should include(""""component_index":1""")
        logs.head should include(""""total_components":3""")
      }

      it("should log component_end event as JSON") {
        val hooks = new LoggingHooks(runId = "comp-end", useStructuredFormat = true)
        hooks.afterComponent(sampleComponentConfig, 1, 3, 250L)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include(""""event":"component_end"""")
        logs.head should include(""""component_name":"TestComponent"""")
        logs.head should include(""""component_index":2""")
        logs.head should include(""""duration_ms":250""")
        logs.head should include(""""status":"success"""")
      }

      it("should log component_error event as JSON") {
        val hooks = new LoggingHooks(runId = "comp-err", useStructuredFormat = true)
        hooks.beforeComponent(sampleComponentConfig, 0, 1) // Start tracking time
        testAppender.clear()

        val error = new IllegalArgumentException("Invalid input")
        hooks.onComponentFailure(sampleComponentConfig, 0, error)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include(""""event":"component_error"""")
        logs.head should include(""""error_type":"IllegalArgumentException"""")
        logs.head should include(""""error_message":"Invalid input"""")
      }

      it("should escape special characters in JSON") {
        val configWithSpecialChars = ComponentConfig(
          instanceType = "com.example.Test",
          instanceName = """Component with "quotes" and \backslash""",
          instanceConfig = ConfigFactory.empty()
        )
        val hooks = new LoggingHooks(runId = "escape", useStructuredFormat = true)
        hooks.beforeComponent(configWithSpecialChars, 0, 1)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include("""Component with \"quotes\" and \\backslash""")
      }
    }

    describe("with human-readable format") {

      it("should log pipeline start in human-readable format") {
        val hooks = new LoggingHooks(runId = "human-1", useStructuredFormat = false)
        hooks.beforePipeline(samplePipelineConfig)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include("Pipeline 'Test Pipeline' starting")
        logs.head should include("run_id=human-1")
        logs.head should include("components=2")
      }

      it("should log pipeline completion in human-readable format") {
        val hooks = new LoggingHooks(runId = "human-2", useStructuredFormat = false)
        hooks.beforePipeline(samplePipelineConfig)
        testAppender.clear()

        hooks.afterPipeline(samplePipelineConfig, PipelineResult.Success(2000L, 2))

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include("Pipeline 'Test Pipeline' completed in 2000ms")
        logs.head should include("run_id=human-2")
      }

      it("should log component progress in human-readable format") {
        val hooks = new LoggingHooks(runId = "human-3", useStructuredFormat = false)
        hooks.beforeComponent(sampleComponentConfig, 1, 5)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include("[2/5] Starting 'TestComponent'")
      }

      it("should log component completion in human-readable format") {
        val hooks = new LoggingHooks(runId = "human-4", useStructuredFormat = false)
        hooks.afterComponent(sampleComponentConfig, 2, 5, 300L)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include("[3/5] Completed 'TestComponent' in 300ms")
      }

      it("should log component failure in human-readable format") {
        val hooks = new LoggingHooks(runId = "human-err", useStructuredFormat = false)
        val error = new NullPointerException("null value")
        hooks.onComponentFailure(sampleComponentConfig, 0, error)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include("Component 'TestComponent' failed")
        logs.head should include("NullPointerException")
        logs.head should include("null value")
      }

      it("should log pipeline failure in human-readable format") {
        val hooks = new LoggingHooks(runId = "human-fail", useStructuredFormat = false)
        hooks.beforePipeline(samplePipelineConfig)
        testAppender.clear()

        val failure = PipelineResult.Failure(
          new RuntimeException("Database connection failed"),
          Some(sampleComponentConfig),
          1
        )
        hooks.afterPipeline(samplePipelineConfig, failure)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include("Pipeline 'Test Pipeline' failed")
        logs.head should include("run_id=human-fail")
      }
    }

    describe("runId generation") {

      it("should auto-generate runId if not provided") {
        val hooks1 = new LoggingHooks()
        val hooks2 = new LoggingHooks()

        hooks1.runId should not be empty
        hooks2.runId should not be empty
        (hooks1.runId should not).equal(hooks2.runId)
      }

      it("should use provided runId") {
        val hooks = new LoggingHooks(runId = "custom-id-123")
        hooks.runId shouldBe "custom-id-123"
      }
    }

    describe("companion object factories") {

      it("should create structured format hooks via LoggingHooks.structured") {
        val hooks = LoggingHooks.structured("struct-id")
        hooks.useStructuredFormat shouldBe true
        hooks.runId shouldBe "struct-id"
      }

      it("should create human-readable format hooks via LoggingHooks.humanReadable") {
        val hooks = LoggingHooks.humanReadable("human-id")
        hooks.useStructuredFormat shouldBe false
        hooks.runId shouldBe "human-id"
      }

      it("should auto-generate runId in factory methods") {
        val structured = LoggingHooks.structured()
        val readable   = LoggingHooks.humanReadable()

        structured.runId should not be empty
        readable.runId should not be empty
      }
    }

    describe("error handling") {

      it("should handle null error message gracefully") {
        val hooks = new LoggingHooks(runId = "null-msg", useStructuredFormat = true)
        val error = new RuntimeException(null: String)
        hooks.onComponentFailure(sampleComponentConfig, 0, error)

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include(""""error_message":"No message"""")
      }

      it("should handle error with newlines in message") {
        val hooks = new LoggingHooks(runId = "newline", useStructuredFormat = true)
        val error = new RuntimeException("Line1\nLine2\rLine3")
        hooks.onComponentFailure(sampleComponentConfig, 0, error)

        val logs = testAppender.getMessages
        logs should have size 1
        // Should be escaped
        logs.head should include("""\n""")
        logs.head should include("""\r""")
      }
    }

    describe("duration tracking") {

      it("should calculate duration for component errors") {
        val hooks = new LoggingHooks(runId = "duration", useStructuredFormat = true)

        // Start component
        hooks.beforeComponent(sampleComponentConfig, 0, 1)

        // Small delay
        Thread.sleep(10)

        testAppender.clear()
        hooks.onComponentFailure(sampleComponentConfig, 0, new RuntimeException("fail"))

        val logs = testAppender.getMessages
        logs should have size 1
        // Duration should be captured (at least 10ms)
        logs.head should include(""""duration_ms":""")
      }

      it("should report zero duration when component error occurs without prior beforeComponent") {
        val hooks = new LoggingHooks(runId = "no-start", useStructuredFormat = true)

        // Call onComponentFailure without beforeComponent
        hooks.onComponentFailure(sampleComponentConfig, 0, new RuntimeException("immediate fail"))

        val logs = testAppender.getMessages
        logs should have size 1
        logs.head should include(""""duration_ms":0""")
      }
    }
  }
}

/** Test appender to capture log messages for verification. */
class TestLogAppender(name: String)
  extends AbstractAppender(
    name,
    null,
    PatternLayout.createDefaultLayout(),
    true,
    Array.empty[Property]
  ) {

  private val messages: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  override def append(event: LogEvent): Unit =
    messages.synchronized {
      messages += event.getMessage.getFormattedMessage
    }

  def getMessages: List[String] = messages.synchronized {
    messages.toList
  }

  def clear(): Unit = messages.synchronized {
    messages.clear()
  }
}
