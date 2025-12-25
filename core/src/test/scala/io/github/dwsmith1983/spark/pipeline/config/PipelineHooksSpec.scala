package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

/** Tests for PipelineHooks trait and companion object. */
class PipelineHooksSpec extends AnyFunSpec with Matchers {

  val samplePipelineConfig: PipelineConfig = PipelineConfig(
    pipelineName = "Test Pipeline",
    pipelineComponents = List.empty
  )

  val sampleComponentConfig: ComponentConfig = ComponentConfig(
    instanceType = "com.example.TestComponent",
    instanceName = "TestComponent",
    instanceConfig = ConfigFactory.empty()
  )

  describe("PipelineHooks.NoOp") {

    it("should not throw when beforePipeline is called") {
      noException should be thrownBy {
        PipelineHooks.NoOp.beforePipeline(samplePipelineConfig)
      }
    }

    it("should not throw when afterPipeline is called") {
      val result: PipelineResult = PipelineResult.Success(100L, 1)

      noException should be thrownBy {
        PipelineHooks.NoOp.afterPipeline(samplePipelineConfig, result)
      }
    }

    it("should not throw when beforeComponent is called") {
      noException should be thrownBy {
        PipelineHooks.NoOp.beforeComponent(sampleComponentConfig, 0, 1)
      }
    }

    it("should not throw when afterComponent is called") {
      noException should be thrownBy {
        PipelineHooks.NoOp.afterComponent(sampleComponentConfig, 0, 1, 50L)
      }
    }

    it("should not throw when onComponentFailure is called") {
      val error: RuntimeException = new RuntimeException("test")

      noException should be thrownBy {
        PipelineHooks.NoOp.onComponentFailure(sampleComponentConfig, 0, error)
      }
    }
  }

  describe("PipelineHooks.compose") {

    it("should call beforePipeline on all hooks in order") {
      val tracker1: TestHooksTracker = new TestHooksTracker("H1")
      val tracker2: TestHooksTracker = new TestHooksTracker("H2")

      val composed: PipelineHooks = PipelineHooks.compose(tracker1, tracker2)
      composed.beforePipeline(samplePipelineConfig)

      tracker1.events shouldBe List("H1:beforePipeline")
      tracker2.events shouldBe List("H2:beforePipeline")
    }

    it("should call afterPipeline on all hooks") {
      val tracker1: TestHooksTracker = new TestHooksTracker("H1")
      val tracker2: TestHooksTracker = new TestHooksTracker("H2")
      val result: PipelineResult     = PipelineResult.Success(100L, 1)

      val composed: PipelineHooks = PipelineHooks.compose(tracker1, tracker2)
      composed.afterPipeline(samplePipelineConfig, result)

      tracker1.events shouldBe List("H1:afterPipeline")
      tracker2.events shouldBe List("H2:afterPipeline")
    }

    it("should call beforeComponent on all hooks") {
      val tracker1: TestHooksTracker = new TestHooksTracker("H1")
      val tracker2: TestHooksTracker = new TestHooksTracker("H2")

      val composed: PipelineHooks = PipelineHooks.compose(tracker1, tracker2)
      composed.beforeComponent(sampleComponentConfig, 0, 3)

      tracker1.events shouldBe List("H1:beforeComponent")
      tracker2.events shouldBe List("H2:beforeComponent")
    }

    it("should call afterComponent on all hooks") {
      val tracker1: TestHooksTracker = new TestHooksTracker("H1")
      val tracker2: TestHooksTracker = new TestHooksTracker("H2")

      val composed: PipelineHooks = PipelineHooks.compose(tracker1, tracker2)
      composed.afterComponent(sampleComponentConfig, 1, 3, 200L)

      tracker1.events shouldBe List("H1:afterComponent")
      tracker2.events shouldBe List("H2:afterComponent")
    }

    it("should call onComponentFailure on all hooks") {
      val tracker1: TestHooksTracker = new TestHooksTracker("H1")
      val tracker2: TestHooksTracker = new TestHooksTracker("H2")
      val error: RuntimeException    = new RuntimeException("test error")

      val composed: PipelineHooks = PipelineHooks.compose(tracker1, tracker2)
      composed.onComponentFailure(sampleComponentConfig, 0, error)

      tracker1.events shouldBe List("H1:onComponentFailure")
      tracker2.events shouldBe List("H2:onComponentFailure")
    }

    it("should handle empty hooks list") {
      val composed: PipelineHooks = PipelineHooks.compose()

      noException should be thrownBy {
        composed.beforePipeline(samplePipelineConfig)
        composed.afterPipeline(samplePipelineConfig, PipelineResult.Success(100L, 0))
      }
    }

    it("should handle single hook") {
      val tracker: TestHooksTracker = new TestHooksTracker("Single")

      val composed: PipelineHooks = PipelineHooks.compose(tracker)
      composed.beforePipeline(samplePipelineConfig)

      tracker.events shouldBe List("Single:beforePipeline")
    }
  }

  describe("default implementations") {

    it("should allow extending with only specific overrides") {
      var beforeCalled: Boolean = false

      val customHooks: PipelineHooks = new PipelineHooks {
        override def beforePipeline(config: PipelineConfig): Unit =
          beforeCalled = true
      }

      customHooks.beforePipeline(samplePipelineConfig)
      beforeCalled shouldBe true

      // Other methods should not throw
      noException should be thrownBy {
        customHooks.afterPipeline(samplePipelineConfig, PipelineResult.Success(100L, 0))
        customHooks.beforeComponent(sampleComponentConfig, 0, 1)
        customHooks.afterComponent(sampleComponentConfig, 0, 1, 50L)
        customHooks.onComponentFailure(sampleComponentConfig, 0, new RuntimeException("test"))
      }
    }
  }

  describe("exception isolation in compose") {

    it("should call all hooks even if first hook throws in beforePipeline") {
      val throwingHook: ThrowingHooks = new ThrowingHooks("Thrower", throwOnBeforePipeline = true)
      val tracker: TestHooksTracker   = new TestHooksTracker("Tracker")

      val composed: PipelineHooks = PipelineHooks.compose(throwingHook, tracker)

      // Should not throw - exceptions should be caught
      noException should be thrownBy {
        composed.beforePipeline(samplePipelineConfig)
      }

      // Second hook should still have been called
      tracker.events should contain("Tracker:beforePipeline")
    }

    it("should call all hooks even if first hook throws in afterPipeline") {
      val throwingHook: ThrowingHooks = new ThrowingHooks("Thrower", throwOnAfterPipeline = true)
      val tracker: TestHooksTracker   = new TestHooksTracker("Tracker")

      val composed: PipelineHooks = PipelineHooks.compose(throwingHook, tracker)

      noException should be thrownBy {
        composed.afterPipeline(samplePipelineConfig, PipelineResult.Success(100L, 1))
      }

      tracker.events should contain("Tracker:afterPipeline")
    }

    it("should call all hooks even if first hook throws in beforeComponent") {
      val throwingHook: ThrowingHooks = new ThrowingHooks("Thrower", throwOnBeforeComponent = true)
      val tracker: TestHooksTracker   = new TestHooksTracker("Tracker")

      val composed: PipelineHooks = PipelineHooks.compose(throwingHook, tracker)

      noException should be thrownBy {
        composed.beforeComponent(sampleComponentConfig, 0, 1)
      }

      tracker.events should contain("Tracker:beforeComponent")
    }

    it("should call all hooks even if first hook throws in afterComponent") {
      val throwingHook: ThrowingHooks = new ThrowingHooks("Thrower", throwOnAfterComponent = true)
      val tracker: TestHooksTracker   = new TestHooksTracker("Tracker")

      val composed: PipelineHooks = PipelineHooks.compose(throwingHook, tracker)

      noException should be thrownBy {
        composed.afterComponent(sampleComponentConfig, 0, 1, 50L)
      }

      tracker.events should contain("Tracker:afterComponent")
    }

    it("should call all hooks even if first hook throws in onComponentFailure") {
      val throwingHook: ThrowingHooks = new ThrowingHooks("Thrower", throwOnComponentFailure = true)
      val tracker: TestHooksTracker   = new TestHooksTracker("Tracker")

      val composed: PipelineHooks = PipelineHooks.compose(throwingHook, tracker)

      noException should be thrownBy {
        composed.onComponentFailure(sampleComponentConfig, 0, new RuntimeException("test"))
      }

      tracker.events should contain("Tracker:onComponentFailure")
    }

    it("should call all hooks when middle hook throws") {
      val tracker1: TestHooksTracker  = new TestHooksTracker("First")
      val throwingHook: ThrowingHooks = new ThrowingHooks("Middle", throwOnBeforePipeline = true)
      val tracker2: TestHooksTracker  = new TestHooksTracker("Last")

      val composed: PipelineHooks = PipelineHooks.compose(tracker1, throwingHook, tracker2)

      noException should be thrownBy {
        composed.beforePipeline(samplePipelineConfig)
      }

      tracker1.events should contain("First:beforePipeline")
      tracker2.events should contain("Last:beforePipeline")
    }
  }

  describe("compose with many hooks") {

    it("should preserve order with more than 2 hooks") {
      val tracker1: TestHooksTracker = new TestHooksTracker("H1")
      val tracker2: TestHooksTracker = new TestHooksTracker("H2")
      val tracker3: TestHooksTracker = new TestHooksTracker("H3")
      val tracker4: TestHooksTracker = new TestHooksTracker("H4")

      val composed: PipelineHooks = PipelineHooks.compose(tracker1, tracker2, tracker3, tracker4)
      composed.beforePipeline(samplePipelineConfig)

      tracker1.events shouldBe List("H1:beforePipeline")
      tracker2.events shouldBe List("H2:beforePipeline")
      tracker3.events shouldBe List("H3:beforePipeline")
      tracker4.events shouldBe List("H4:beforePipeline")
    }
  }

  describe("hooks reuse") {

    it("should work correctly when same hooks instance is used for multiple pipelines") {
      val tracker: TestHooksTracker = new TestHooksTracker("Reused")

      // First pipeline
      tracker.events.clear()
      tracker.beforePipeline(samplePipelineConfig)
      tracker.afterPipeline(samplePipelineConfig, PipelineResult.Success(100L, 0))

      tracker.events shouldBe List("Reused:beforePipeline", "Reused:afterPipeline")

      // Second pipeline - should still work
      tracker.events.clear()
      tracker.beforePipeline(samplePipelineConfig)
      tracker.beforeComponent(sampleComponentConfig, 0, 1)
      tracker.afterComponent(sampleComponentConfig, 0, 1, 50L)
      tracker.afterPipeline(samplePipelineConfig, PipelineResult.Success(150L, 1))

      tracker.events shouldBe List(
        "Reused:beforePipeline",
        "Reused:beforeComponent",
        "Reused:afterComponent",
        "Reused:afterPipeline"
      )
    }
  }
}

/** Test helper that throws exceptions on specified hook methods. */
class ThrowingHooks(
  name: String,
  throwOnBeforePipeline: Boolean = false,
  throwOnAfterPipeline: Boolean = false,
  throwOnBeforeComponent: Boolean = false,
  throwOnAfterComponent: Boolean = false,
  throwOnComponentFailure: Boolean = false) extends PipelineHooks {

  override def beforePipeline(config: PipelineConfig): Unit = {
    val _ = config
    if (throwOnBeforePipeline) throw new RuntimeException(s"$name: beforePipeline error")
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    val _ = (config, result)
    if (throwOnAfterPipeline) throw new RuntimeException(s"$name: afterPipeline error")
  }

  override def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit = {
    val _ = (config, index, total)
    if (throwOnBeforeComponent) throw new RuntimeException(s"$name: beforeComponent error")
  }

  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    val _ = (config, index, total, durationMs)
    if (throwOnAfterComponent) throw new RuntimeException(s"$name: afterComponent error")
  }

  override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {
    val _ = (config, index, error)
    if (throwOnComponentFailure) throw new RuntimeException(s"$name: onComponentFailure error")
  }
}

/** Test helper to track hook invocations. */
class TestHooksTracker(name: String) extends PipelineHooks {
  val events: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  override def beforePipeline(config: PipelineConfig): Unit = {
    val _ = config
    events += s"$name:beforePipeline"
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    val _ = (config, result)
    events += s"$name:afterPipeline"
  }

  override def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit = {
    val _ = (config, index, total)
    events += s"$name:beforeComponent"
  }

  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    val _ = (config, index, total, durationMs)
    events += s"$name:afterComponent"
  }

  override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {
    val _ = (config, index, error)
    events += s"$name:onComponentFailure"
  }
}
