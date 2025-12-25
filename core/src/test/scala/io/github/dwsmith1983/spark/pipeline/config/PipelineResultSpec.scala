package io.github.dwsmith1983.spark.pipeline.config

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Tests for PipelineResult sealed trait and its case classes. */
class PipelineResultSpec extends AnyFunSpec with Matchers {

  describe("PipelineResult.Success") {

    it("should report isSuccess as true") {
      val result: PipelineResult = PipelineResult.Success(durationMs = 1000L, componentsRun = 3)

      result.isSuccess shouldBe true
    }

    it("should report isFailure as false") {
      val result: PipelineResult = PipelineResult.Success(durationMs = 500L, componentsRun = 1)

      result.isFailure shouldBe false
    }

    it("should store duration and components count") {
      val result: PipelineResult.Success = PipelineResult.Success(durationMs = 2500L, componentsRun = 5)

      result.durationMs shouldBe 2500L
      result.componentsRun shouldBe 5
    }
  }

  describe("PipelineResult.Failure") {

    it("should report isSuccess as false") {
      val error: RuntimeException = new RuntimeException("test error")
      val result: PipelineResult  = PipelineResult.Failure(error, failedComponent = None, componentsCompleted = 0)

      result.isSuccess shouldBe false
    }

    it("should report isFailure as true") {
      val error: RuntimeException = new RuntimeException("test error")
      val result: PipelineResult  = PipelineResult.Failure(error, failedComponent = None, componentsCompleted = 0)

      result.isFailure shouldBe true
    }

    it("should store error and failed component") {
      val error: RuntimeException = new RuntimeException("component failed")
      val componentConfig: ComponentConfig = ComponentConfig(
        instanceType = "com.example.MyComponent",
        instanceName = "FailedComponent",
        instanceConfig = com.typesafe.config.ConfigFactory.empty()
      )
      val result: PipelineResult.Failure =
        PipelineResult.Failure(error, failedComponent = Some(componentConfig), componentsCompleted = 2)

      result.error shouldBe error
      result.failedComponent shouldBe Some(componentConfig)
      result.componentsCompleted shouldBe 2
    }

    it("should allow None for failedComponent") {
      val error: RuntimeException = new RuntimeException("config error")
      val result: PipelineResult.Failure = PipelineResult.Failure(error, failedComponent = None, componentsCompleted = 0)

      result.failedComponent shouldBe None
    }
  }

  describe("PipelineResult.PartialSuccess") {

    it("should report isSuccess as false") {
      val result: PipelineResult = PipelineResult.PartialSuccess(
        durationMs = 1000L,
        componentsSucceeded = 2,
        componentsFailed = 1,
        failures = List.empty
      )

      result.isSuccess shouldBe false
    }

    it("should report isFailure as true") {
      val result: PipelineResult = PipelineResult.PartialSuccess(
        durationMs = 1000L,
        componentsSucceeded = 2,
        componentsFailed = 1,
        failures = List.empty
      )

      result.isFailure shouldBe true
    }

    it("should store all fields correctly") {
      val componentConfig: ComponentConfig = ComponentConfig(
        instanceType = "com.example.FailedComponent",
        instanceName = "FailedOne",
        instanceConfig = com.typesafe.config.ConfigFactory.empty()
      )
      val error: RuntimeException = new RuntimeException("component error")

      val result: PipelineResult.PartialSuccess = PipelineResult.PartialSuccess(
        durationMs = 2500L,
        componentsSucceeded = 3,
        componentsFailed = 2,
        failures = List((componentConfig, error))
      )

      result.durationMs shouldBe 2500L
      result.componentsSucceeded shouldBe 3
      result.componentsFailed shouldBe 2
      result.failures should have size 1
      result.failures.head._1.instanceName shouldBe "FailedOne"
      result.failures.head._2 shouldBe error
    }

    it("should handle empty failures list") {
      val result: PipelineResult.PartialSuccess = PipelineResult.PartialSuccess(
        durationMs = 500L,
        componentsSucceeded = 1,
        componentsFailed = 0,
        failures = List.empty
      )

      result.failures shouldBe empty
    }

    it("should handle multiple failures") {
      val comp1: ComponentConfig = ComponentConfig(
        instanceType = "com.example.Fail1",
        instanceName = "Fail1",
        instanceConfig = com.typesafe.config.ConfigFactory.empty()
      )
      val comp2: ComponentConfig = ComponentConfig(
        instanceType = "com.example.Fail2",
        instanceName = "Fail2",
        instanceConfig = com.typesafe.config.ConfigFactory.empty()
      )
      val error1: RuntimeException = new RuntimeException("error 1")
      val error2: RuntimeException = new RuntimeException("error 2")

      val result: PipelineResult.PartialSuccess = PipelineResult.PartialSuccess(
        durationMs = 3000L,
        componentsSucceeded = 1,
        componentsFailed = 2,
        failures = List((comp1, error1), (comp2, error2))
      )

      result.failures should have size 2
      result.failures.map(_._1.instanceName) shouldBe List("Fail1", "Fail2")
    }
  }

  describe("edge cases") {

    it("should handle zero duration in Success") {
      val result: PipelineResult.Success = PipelineResult.Success(durationMs = 0L, componentsRun = 0)

      result.durationMs shouldBe 0L
      result.componentsRun shouldBe 0
      result.isSuccess shouldBe true
    }

    it("should handle zero duration in Failure") {
      val error: RuntimeException        = new RuntimeException("instant fail")
      val result: PipelineResult.Failure = PipelineResult.Failure(error, None, 0)

      result.isFailure shouldBe true
    }

    it("should handle zero duration in PartialSuccess") {
      val result: PipelineResult.PartialSuccess = PipelineResult.PartialSuccess(
        durationMs = 0L,
        componentsSucceeded = 0,
        componentsFailed = 0,
        failures = List.empty
      )

      result.durationMs shouldBe 0L
      result.isSuccess shouldBe false
    }

    it("should handle large duration values") {
      val result: PipelineResult.Success = PipelineResult.Success(
        durationMs = Long.MaxValue,
        componentsRun = Int.MaxValue
      )

      result.durationMs shouldBe Long.MaxValue
      result.componentsRun shouldBe Int.MaxValue
    }
  }
}
