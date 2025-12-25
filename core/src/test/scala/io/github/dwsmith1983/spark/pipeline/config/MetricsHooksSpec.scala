package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.ConfigFactory
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.core.instrument.Tags
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.util.concurrent.TimeUnit

/** Tests for MetricsHooks. */
class MetricsHooksSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  var registry: SimpleMeterRegistry = _
  var hooks: MetricsHooks           = _

  val testPipelineConfig: PipelineConfig = PipelineConfig(
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

  val testComponentConfig: ComponentConfig = testPipelineConfig.pipelineComponents.head

  override def beforeEach(): Unit = {
    registry = new SimpleMeterRegistry()
    hooks = new MetricsHooks(registry)
  }

  describe("MetricsHooks") {

    describe("happy path") {

      it("should record pipeline duration on successful completion") {
        hooks.beforePipeline(testPipelineConfig)
        Thread.sleep(10)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 2))

        val timer = registry.find("pipeline.duration")
          .tag("pipeline_name", "Test Pipeline")
          .tag("status", "success")
          .timer()

        timer should not be null
        timer.count() shouldBe 1
        timer.totalTime(TimeUnit.MILLISECONDS) shouldBe 100.0 +- 1.0
      }

      it("should record pipeline runs counter on completion") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 2))

        val counter = registry.find("pipeline.runs")
          .tag("pipeline_name", "Test Pipeline")
          .tag("status", "success")
          .counter()

        counter should not be null
        counter.count() shouldBe 1.0
      }

      it("should record component duration on successful completion") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.afterComponent(testComponentConfig, 0, 2, 50L)

        val timer = registry.find("pipeline.component.duration")
          .tag("pipeline_name", "Test Pipeline")
          .tag("component_name", "Component1")
          .tag("status", "success")
          .timer()

        timer should not be null
        timer.count() shouldBe 1
        timer.totalTime(TimeUnit.MILLISECONDS) shouldBe 50.0 +- 1.0
      }

      it("should record component runs counter on completion") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.afterComponent(testComponentConfig, 0, 2, 50L)

        val counter = registry.find("pipeline.component.runs")
          .tag("pipeline_name", "Test Pipeline")
          .tag("component_name", "Component1")
          .tag("status", "success")
          .counter()

        counter should not be null
        counter.count() shouldBe 1.0
      }

      it("should increment counters on multiple runs") {
        // First run
        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 2))

        // Second run
        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(150L, 2))

        val counter = registry.find("pipeline.runs")
          .tag("status", "success")
          .counter()

        counter.count() shouldBe 2.0
      }
    }

    describe("failure handling") {

      it("should record component failure metrics") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        Thread.sleep(10)
        hooks.onComponentFailure(testComponentConfig, 0, new RuntimeException("test error"))

        val timer = registry.find("pipeline.component.duration")
          .tag("component_name", "Component1")
          .tag("status", "failure")
          .timer()

        timer should not be null
        timer.count() shouldBe 1
        timer.totalTime(TimeUnit.MILLISECONDS) should be >= 10.0
      }

      it("should record pipeline failure metrics") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(
          testPipelineConfig,
          PipelineResult.Failure(new RuntimeException("test"), None, 0)
        )

        val counter = registry.find("pipeline.runs")
          .tag("status", "failure")
          .counter()

        counter should not be null
        counter.count() shouldBe 1.0
      }

      it("should record partial success metrics") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(
          testPipelineConfig,
          PipelineResult.PartialSuccess(200L, 1, 1, List.empty)
        )

        val counter = registry.find("pipeline.runs")
          .tag("status", "partial_success")
          .counter()

        counter should not be null
        counter.count() shouldBe 1.0
      }

      it("should handle component failure without prior beforeComponent") {
        hooks.beforePipeline(testPipelineConfig)
        // Skip beforeComponent
        hooks.onComponentFailure(testComponentConfig, 0, new RuntimeException("immediate fail"))

        val timer = registry.find("pipeline.component.duration")
          .tag("status", "failure")
          .timer()

        timer should not be null
        timer.count() shouldBe 1
        // Duration should be >= 0 (fallback to current time when no start recorded)
        timer.totalTime(TimeUnit.MILLISECONDS) should be >= 0.0
      }
    }

    describe("missing beforePipeline scenarios") {

      it("should use 'unknown' pipeline name when afterPipeline called without beforePipeline") {
        // Don't call beforePipeline - currentPipelineConfig will be null
        val freshHooks = new MetricsHooks(registry)
        freshHooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 2))

        val counter = registry.find("pipeline.runs")
          .tag("pipeline_name", "unknown")
          .tag("status", "success")
          .counter()

        counter should not be null
        counter.count() shouldBe 1.0
      }

      it("should use 'unknown' pipeline name when recording component metrics without beforePipeline") {
        val freshHooks = new MetricsHooks(registry)
        // Skip beforePipeline, go directly to component
        freshHooks.afterComponent(testComponentConfig, 0, 1, 50L)

        val timer = registry.find("pipeline.component.duration")
          .tag("pipeline_name", "unknown")
          .tag("component_name", "Component1")
          .timer()

        timer should not be null
        timer.count() shouldBe 1
      }

      it("should use 'unknown' pipeline name for component failure without beforePipeline") {
        val freshHooks = new MetricsHooks(registry)
        freshHooks.onComponentFailure(testComponentConfig, 0, new RuntimeException("fail"))

        val counter = registry.find("pipeline.component.runs")
          .tag("pipeline_name", "unknown")
          .tag("status", "failure")
          .counter()

        counter should not be null
        counter.count() shouldBe 1.0
      }
    }

    describe("state management") {

      it("should properly reset state between pipeline runs") {
        // First pipeline run
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.afterComponent(testComponentConfig, 0, 2, 50L)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 1))

        // Second pipeline with different name
        val secondPipeline = PipelineConfig(
          pipelineName = "Second Pipeline",
          pipelineComponents = List(testComponentConfig)
        )
        hooks.beforePipeline(secondPipeline)
        hooks.beforeComponent(testComponentConfig, 0, 1)
        hooks.afterComponent(testComponentConfig, 0, 1, 75L)
        hooks.afterPipeline(secondPipeline, PipelineResult.Success(150L, 1))

        // Verify both pipelines recorded separately
        val firstCounter = registry.find("pipeline.runs")
          .tag("pipeline_name", "Test Pipeline")
          .counter()
        val secondCounter = registry.find("pipeline.runs")
          .tag("pipeline_name", "Second Pipeline")
          .counter()

        firstCounter.count() shouldBe 1.0
        secondCounter.count() shouldBe 1.0
      }

      it("should not create failure metrics after successful component") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.afterComponent(testComponentConfig, 0, 2, 50L)

        // Verify success metrics exist
        val successCounter = registry.find("pipeline.component.runs")
          .tag("component_name", "Component1")
          .tag("status", "success")
          .counter()
        successCounter should not be null
        successCounter.count() shouldBe 1.0

        // Verify failure metrics do NOT exist for this component
        val failureCounter = registry.find("pipeline.component.runs")
          .tag("component_name", "Component1")
          .tag("status", "failure")
          .counter()
        failureCounter shouldBe null
      }

      it("should not create success metrics after failed component") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testComponentConfig, 0, 2)
        hooks.onComponentFailure(testComponentConfig, 0, new RuntimeException("fail"))

        // Verify failure metrics exist
        val failureCounter = registry.find("pipeline.component.runs")
          .tag("component_name", "Component1")
          .tag("status", "failure")
          .counter()
        failureCounter should not be null
        failureCounter.count() shouldBe 1.0

        // Verify success metrics do NOT exist for this component
        val successCounter = registry.find("pipeline.component.runs")
          .tag("component_name", "Component1")
          .tag("status", "success")
          .counter()
        successCounter shouldBe null
      }
    }

    describe("lifecycle integration") {

      it("should correctly record metrics for mixed success/failure pipeline") {
        val component1 = testComponentConfig
        val component2 = testPipelineConfig.pipelineComponents(1)

        hooks.beforePipeline(testPipelineConfig)

        // Component 1 succeeds
        hooks.beforeComponent(component1, 0, 2)
        hooks.afterComponent(component1, 0, 2, 100L)

        // Component 2 fails
        hooks.beforeComponent(component2, 1, 2)
        hooks.onComponentFailure(component2, 1, new RuntimeException("component 2 failed"))

        // Pipeline ends with partial success
        hooks.afterPipeline(
          testPipelineConfig,
          PipelineResult.PartialSuccess(200L, 1, 1, List.empty)
        )

        // Verify component 1 recorded as success
        val comp1Timer = registry.find("pipeline.component.duration")
          .tag("component_name", "Component1")
          .tag("status", "success")
          .timer()
        comp1Timer should not be null
        comp1Timer.count() shouldBe 1
        comp1Timer.totalTime(TimeUnit.MILLISECONDS) shouldBe 100.0 +- 1.0

        // Verify component 2 recorded as failure
        val comp2Counter = registry.find("pipeline.component.runs")
          .tag("component_name", "Component2")
          .tag("status", "failure")
          .counter()
        comp2Counter should not be null
        comp2Counter.count() shouldBe 1.0

        // Verify pipeline recorded as partial_success
        val pipelineCounter = registry.find("pipeline.runs")
          .tag("pipeline_name", "Test Pipeline")
          .tag("status", "partial_success")
          .counter()
        pipelineCounter should not be null
        pipelineCounter.count() shouldBe 1.0

        // Verify timer accumulates correctly
        val pipelineTimer = registry.find("pipeline.duration")
          .tag("status", "partial_success")
          .timer()
        pipelineTimer.totalTime(TimeUnit.MILLISECONDS) shouldBe 200.0 +- 1.0
      }

      it("should accumulate timer values across multiple recordings") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 1))

        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(150L, 1))

        hooks.beforePipeline(testPipelineConfig)
        hooks.afterPipeline(testPipelineConfig, PipelineResult.Success(50L, 1))

        val timer = registry.find("pipeline.duration")
          .tag("status", "success")
          .timer()

        timer.count() shouldBe 3
        timer.totalTime(TimeUnit.MILLISECONDS) shouldBe 300.0 +- 1.0
      }
    }

    describe("custom configuration") {

      it("should use custom metrics prefix") {
        val customHooks = MetricsHooks.withPrefix(registry, "myapp.pipeline")

        customHooks.beforePipeline(testPipelineConfig)
        customHooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 2))

        val timer = registry.find("myapp.pipeline.duration")
          .tag("status", "success")
          .timer()

        timer should not be null
        timer.count() shouldBe 1
      }

      it("should apply common tags to all metrics") {
        val customTags  = Tags.of("env", "test", "service", "data-pipeline")
        val customHooks = MetricsHooks.withTags(registry, "pipeline", customTags)

        customHooks.beforePipeline(testPipelineConfig)
        customHooks.beforeComponent(testComponentConfig, 0, 2)
        customHooks.afterComponent(testComponentConfig, 0, 2, 50L)
        customHooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 2))

        // Check pipeline timer has custom tags
        val pipelineTimer = registry.find("pipeline.duration")
          .tag("env", "test")
          .tag("service", "data-pipeline")
          .timer()
        pipelineTimer should not be null

        // Check component timer has custom tags
        val componentTimer = registry.find("pipeline.component.duration")
          .tag("env", "test")
          .tag("service", "data-pipeline")
          .timer()
        componentTimer should not be null
      }
    }

    describe("edge cases") {

      it("should handle pipeline with no components") {
        val emptyPipeline = PipelineConfig(
          pipelineName = "Empty Pipeline",
          pipelineComponents = List.empty
        )

        hooks.beforePipeline(emptyPipeline)
        hooks.afterPipeline(emptyPipeline, PipelineResult.Success(0L, 0))

        val counter = registry.find("pipeline.runs")
          .tag("pipeline_name", "Empty Pipeline")
          .counter()
        counter should not be null
        counter.count() shouldBe 1.0
      }

      it("should handle special characters in component names") {
        val specialConfig = ComponentConfig(
          instanceType = "com.example.Test",
          instanceName = "Component with spaces & special-chars!",
          instanceConfig = ConfigFactory.empty()
        )

        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(specialConfig, 0, 1)
        hooks.afterComponent(specialConfig, 0, 1, 50L)

        val timer = registry.find("pipeline.component.duration")
          .tag("component_name", "Component with spaces & special-chars!")
          .timer()
        timer should not be null
      }

      it("should handle empty pipeline name") {
        val emptyNamePipeline = PipelineConfig(
          pipelineName = "",
          pipelineComponents = List(testComponentConfig)
        )

        hooks.beforePipeline(emptyNamePipeline)
        hooks.afterPipeline(emptyNamePipeline, PipelineResult.Success(100L, 1))

        val counter = registry.find("pipeline.runs")
          .tag("pipeline_name", "")
          .tag("status", "success")
          .counter()
        counter should not be null
        counter.count() shouldBe 1.0
      }

      it("should handle empty component name") {
        val emptyNameComponent = ComponentConfig(
          instanceType = "com.example.Test",
          instanceName = "",
          instanceConfig = ConfigFactory.empty()
        )

        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(emptyNameComponent, 0, 1)
        hooks.afterComponent(emptyNameComponent, 0, 1, 50L)

        val timer = registry.find("pipeline.component.duration")
          .tag("component_name", "")
          .timer()
        timer should not be null
        timer.count() shouldBe 1
      }
    }
  }
}
