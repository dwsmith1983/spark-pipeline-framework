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
}
