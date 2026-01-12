package io.github.dwsmith1983.spark.pipeline.dq

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/** Tests for CheckTiming, FailureMode, and DataQualityException. */
class DataQualityConfigSpec extends AnyFunSpec with Matchers {

  val timestamp: Instant = Instant.parse("2024-01-01T00:00:00Z")

  describe("CheckTiming") {

    describe("BeforePipeline") {

      it("should be a case object") {
        CheckTiming.BeforePipeline shouldBe a[CheckTiming]
      }

      it("should be equal to itself") {
        CheckTiming.BeforePipeline shouldBe CheckTiming.BeforePipeline
      }
    }

    describe("AfterPipeline") {

      it("should be a case object") {
        CheckTiming.AfterPipeline shouldBe a[CheckTiming]
      }

      it("should be different from BeforePipeline") {
        CheckTiming.AfterPipeline should not be CheckTiming.BeforePipeline
      }
    }

    describe("AfterComponent") {

      it("should store component name") {
        val timing = CheckTiming.AfterComponent("LoadData")
        timing.componentName shouldBe "LoadData"
      }

      it("should match equal instances") {
        val t1 = CheckTiming.AfterComponent("Transform")
        val t2 = CheckTiming.AfterComponent("Transform")
        t1 shouldBe t2
      }

      it("should differ for different component names") {
        val t1 = CheckTiming.AfterComponent("Step1")
        val t2 = CheckTiming.AfterComponent("Step2")
        t1 should not be t2
      }

      it("should handle empty component name") {
        val timing = CheckTiming.AfterComponent("")
        timing.componentName shouldBe ""
      }

      it("should handle special characters in name") {
        val timing = CheckTiming.AfterComponent("schema.component-v2")
        timing.componentName shouldBe "schema.component-v2"
      }
    }

    describe("pattern matching") {

      it("should support exhaustive pattern matching") {
        def describe(timing: CheckTiming): String = timing match {
          case CheckTiming.BeforePipeline          => "before"
          case CheckTiming.AfterPipeline           => "after"
          case CheckTiming.AfterComponent(name)    => s"after:$name"
        }

        describe(CheckTiming.BeforePipeline) shouldBe "before"
        describe(CheckTiming.AfterPipeline) shouldBe "after"
        describe(CheckTiming.AfterComponent("X")) shouldBe "after:X"
      }
    }
  }

  describe("FailureMode") {

    describe("FailOnError") {

      it("should be a case object") {
        FailureMode.FailOnError shouldBe a[FailureMode]
      }
    }

    describe("WarnOnly") {

      it("should be a case object") {
        FailureMode.WarnOnly shouldBe a[FailureMode]
      }

      it("should be different from FailOnError") {
        FailureMode.WarnOnly should not be FailureMode.FailOnError
      }
    }

    describe("Threshold") {

      it("should store max failures") {
        val mode = FailureMode.Threshold(3)
        mode.maxFailures shouldBe 3
      }

      it("should accept zero threshold") {
        val mode = FailureMode.Threshold(0)
        mode.maxFailures shouldBe 0
      }

      it("should reject negative threshold") {
        an[IllegalArgumentException] should be thrownBy {
          FailureMode.Threshold(-1)
        }
      }

      it("should handle large threshold") {
        val mode = FailureMode.Threshold(Int.MaxValue)
        mode.maxFailures shouldBe Int.MaxValue
      }

      it("should match equal instances") {
        val m1 = FailureMode.Threshold(5)
        val m2 = FailureMode.Threshold(5)
        m1 shouldBe m2
      }

      it("should differ for different thresholds") {
        val m1 = FailureMode.Threshold(1)
        val m2 = FailureMode.Threshold(2)
        m1 should not be m2
      }
    }

    describe("pattern matching") {

      it("should support exhaustive pattern matching") {
        def describe(mode: FailureMode): String = mode match {
          case FailureMode.FailOnError  => "fail"
          case FailureMode.WarnOnly     => "warn"
          case FailureMode.Threshold(n) => s"threshold:$n"
        }

        describe(FailureMode.FailOnError) shouldBe "fail"
        describe(FailureMode.WarnOnly) shouldBe "warn"
        describe(FailureMode.Threshold(3)) shouldBe "threshold:3"
      }
    }
  }

  describe("DataQualityException") {

    describe("single check failure") {

      it("should store check name and table name") {
        val ex = DataQualityException("row_count", "users", "Too few rows")
        ex.checkName shouldBe "row_count"
        ex.tableName shouldBe "users"
      }

      it("should format message correctly") {
        val ex = DataQualityException("null_check", "orders", "5% nulls found")
        ex.getMessage should include("null_check")
        ex.getMessage should include("orders")
        ex.getMessage should include("5% nulls found")
      }

      it("should have default failed count of 1") {
        val ex = DataQualityException("check", "table", "error")
        ex.failedCount shouldBe 1
      }

      it("should be a RuntimeException") {
        val ex = DataQualityException("check", "table", "error")
        ex shouldBe a[RuntimeException]
      }
    }

    describe("multiple check failures") {

      it("should create exception from multiple failures") {
        val failures = Seq(
          DataQualityResult.Failed("check1", "table1", "error1", Map.empty, timestamp),
          DataQualityResult.Failed("check2", "table2", "error2", Map.empty, timestamp),
          DataQualityResult.Failed("check3", "table3", "error3", Map.empty, timestamp)
        )

        val ex = DataQualityException.multiple(failures)
        ex.failedCount shouldBe 3
        ex.getMessage should include("3 checks failed")
      }

      it("should use first failure info") {
        val failures = Seq(
          DataQualityResult.Failed("first_check", "first_table", "first error", Map.empty, timestamp),
          DataQualityResult.Failed("second_check", "second_table", "second error", Map.empty, timestamp)
        )

        val ex = DataQualityException.multiple(failures)
        ex.checkName shouldBe "first_check"
        ex.tableName shouldBe "first_table"
      }

      it("should handle single failure in sequence") {
        val failures = Seq(
          DataQualityResult.Failed("only_check", "only_table", "single error", Map.empty, timestamp)
        )

        val ex = DataQualityException.multiple(failures)
        ex.failedCount shouldBe 1
        ex.getMessage shouldBe "Data quality check 'only_check' failed on table 'only_table': single error"
      }
    }

    describe("edge cases") {

      it("should handle empty strings") {
        val ex = DataQualityException("", "", "")
        ex.checkName shouldBe ""
        ex.tableName shouldBe ""
      }

      it("should handle special characters in message") {
        val ex = DataQualityException("check", "table", "Error: 'quotes' and \"double quotes\"")
        ex.getMessage should include("'quotes'")
        ex.getMessage should include("\"double quotes\"")
      }

      it("should handle unicode in message") {
        val ex = DataQualityException("check", "table", "Error: 日本語 🚀")
        ex.getMessage should include("日本語")
        ex.getMessage should include("🚀")
      }
    }
  }
}
