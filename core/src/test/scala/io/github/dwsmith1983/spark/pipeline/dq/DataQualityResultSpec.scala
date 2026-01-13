package io.github.dwsmith1983.spark.pipeline.dq

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/** Tests for DataQualityResult. */
class DataQualityResultSpec extends AnyFunSpec with Matchers {

  val timestamp: Instant = Instant.parse("2024-01-01T00:00:00Z")

  describe("DataQualityResult.Passed") {

    it("should correctly identify isPassed as true") {
      val result = DataQualityResult.Passed("test_check", "test_table", Map.empty, timestamp)
      result.isPassed shouldBe true
      result.isFailed shouldBe false
      result.isWarning shouldBe false
      result.isSkipped shouldBe false
    }

    it("should store check name and table name") {
      val result = DataQualityResult.Passed("row_count", "users", Map.empty, timestamp)
      result.checkName shouldBe "row_count"
      result.tableName shouldBe "users"
    }

    it("should store details map") {
      val details = Map[String, Any]("count" -> 100L, "threshold" -> 50L)
      val result  = DataQualityResult.Passed("test", "table", details, timestamp)
      result.details shouldBe details
    }

    it("should use default empty details when not provided") {
      val result = DataQualityResult.Passed("test", "table", timestamp = timestamp)
      result.details shouldBe Map.empty
    }
  }

  describe("DataQualityResult.Failed") {

    it("should correctly identify isFailed as true") {
      val result = DataQualityResult.Failed("test_check", "test_table", "failure message", Map.empty, timestamp)
      result.isFailed shouldBe true
      result.isPassed shouldBe false
      result.isWarning shouldBe false
      result.isSkipped shouldBe false
    }

    it("should store failure message") {
      val result = DataQualityResult.Failed("check", "table", "Row count below minimum", Map.empty, timestamp)
      result.message shouldBe "Row count below minimum"
    }

    it("should store details with failure info") {
      val details = Map[String, Any]("actual" -> 10L, "expected" -> 100L)
      val result  = DataQualityResult.Failed("check", "table", "failure", details, timestamp)
      result.details shouldBe details
    }
  }

  describe("DataQualityResult.Warning") {

    it("should correctly identify isWarning as true") {
      val result = DataQualityResult.Warning("test_check", "test_table", "warning message", Map.empty, timestamp)
      result.isWarning shouldBe true
      result.isPassed shouldBe false
      result.isFailed shouldBe false
      result.isSkipped shouldBe false
    }

    it("should store warning message") {
      val result = DataQualityResult.Warning("check", "table", "Approaching threshold", Map.empty, timestamp)
      result.message shouldBe "Approaching threshold"
    }
  }

  describe("DataQualityResult.Skipped") {

    it("should correctly identify isSkipped as true") {
      val result = DataQualityResult.Skipped("test_check", "test_table", "reason", timestamp)
      result.isSkipped shouldBe true
      result.isPassed shouldBe false
      result.isFailed shouldBe false
      result.isWarning shouldBe false
    }

    it("should store skip reason") {
      val result = DataQualityResult.Skipped("check", "missing_table", "Table not found", timestamp)
      result.reason shouldBe "Table not found"
    }
  }

  describe("timestamp handling") {

    it("should use provided timestamp") {
      val ts     = Instant.parse("2024-06-15T10:30:00Z")
      val result = DataQualityResult.Passed("test", "table", Map.empty, ts)
      result.timestamp shouldBe ts
    }

    it("should use current time when not provided") {
      val before = Instant.now()
      val result = DataQualityResult.Passed("test", "table")
      val after  = Instant.now()

      result.timestamp should not be null
      result.timestamp.toEpochMilli should be >= before.toEpochMilli
      result.timestamp.toEpochMilli should be <= after.toEpochMilli
    }
  }

  describe("edge cases") {

    it("should handle empty check name") {
      val result = DataQualityResult.Passed("", "table", Map.empty, timestamp)
      result.checkName shouldBe ""
    }

    it("should handle empty table name") {
      val result = DataQualityResult.Passed("check", "", Map.empty, timestamp)
      result.tableName shouldBe ""
    }

    it("should handle special characters in names") {
      val result = DataQualityResult.Passed("check_with_special-chars.v2", "schema.table", Map.empty, timestamp)
      result.checkName shouldBe "check_with_special-chars.v2"
      result.tableName shouldBe "schema.table"
    }

    it("should handle unicode in message") {
      val result = DataQualityResult.Failed("check", "table", "Error: 日本語 émoji 🚀", Map.empty, timestamp)
      result.message should include("日本語")
    }

    it("should handle details with various types") {
      val details = Map[String, Any](
        "string_val"  -> "text",
        "int_val"     -> 42,
        "long_val"    -> 1000L,
        "double_val"  -> 3.14,
        "boolean_val" -> true
      )
      val result = DataQualityResult.Passed("check", "table", details, timestamp)
      result.details("string_val") shouldBe "text"
      result.details("int_val") shouldBe 42
      result.details("long_val") shouldBe 1000L
      result.details("double_val") shouldBe 3.14
      result.details("boolean_val") shouldBe true
    }
  }
}
