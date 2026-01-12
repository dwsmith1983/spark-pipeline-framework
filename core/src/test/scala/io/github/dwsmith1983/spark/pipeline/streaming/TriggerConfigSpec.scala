package io.github.dwsmith1983.spark.pipeline.streaming

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Tests for TriggerConfig ADT. */
class TriggerConfigSpec extends AnyFunSpec with Matchers {

  describe("TriggerConfig") {

    describe("ProcessingTime") {

      it("should create with interval string") {
        val trigger = TriggerConfig.ProcessingTime("10 seconds")

        trigger.interval shouldBe "10 seconds"
      }

      it("should have correct description") {
        val trigger = TriggerConfig.ProcessingTime("1 minute")

        trigger.description shouldBe "ProcessingTime(1 minute)"
      }

      it("should support various interval formats") {
        val intervals = Seq("0 seconds", "100 milliseconds", "5 minutes", "1 hour")

        intervals.foreach { interval =>
          val trigger = TriggerConfig.ProcessingTime(interval)
          trigger.interval shouldBe interval
        }
      }
    }

    describe("Once") {

      it("should be a singleton object") {
        TriggerConfig.Once shouldBe TriggerConfig.Once
      }

      it("should have correct description") {
        TriggerConfig.Once.description shouldBe "Once"
      }
    }

    describe("Continuous") {

      it("should create with checkpoint interval") {
        val trigger = TriggerConfig.Continuous("1 second")

        trigger.checkpointInterval shouldBe "1 second"
      }

      it("should have correct description") {
        val trigger = TriggerConfig.Continuous("500 milliseconds")

        trigger.description shouldBe "Continuous(500 milliseconds)"
      }
    }

    describe("AvailableNow") {

      it("should be a singleton object") {
        TriggerConfig.AvailableNow shouldBe TriggerConfig.AvailableNow
      }

      it("should have correct description") {
        TriggerConfig.AvailableNow.description shouldBe "AvailableNow"
      }
    }

    describe("pattern matching") {

      it("should support exhaustive pattern matching") {
        def describe(trigger: TriggerConfig): String = trigger match {
          case TriggerConfig.ProcessingTime(interval) => s"process every $interval"
          case TriggerConfig.Once                     => "run once"
          case TriggerConfig.Continuous(interval)     => s"continuous with $interval checkpoint"
          case TriggerConfig.AvailableNow             => "process available data"
        }

        describe(TriggerConfig.ProcessingTime("10s")) shouldBe "process every 10s"
        describe(TriggerConfig.Once) shouldBe "run once"
        describe(TriggerConfig.Continuous("1s")) shouldBe "continuous with 1s checkpoint"
        describe(TriggerConfig.AvailableNow) shouldBe "process available data"
      }
    }
  }
}
