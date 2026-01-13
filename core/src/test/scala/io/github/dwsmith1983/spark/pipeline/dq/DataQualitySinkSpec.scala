package io.github.dwsmith1983.spark.pipeline.dq

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.io.File
import java.nio.file.Files
import java.time.Instant
import scala.io.Source

/** Tests for DataQualitySink and related classes. */
class DataQualitySinkSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  var tempDir: File   = _
  var tempFile: File  = _
  val timestamp: Instant = Instant.parse("2024-01-01T00:00:00Z")

  override def beforeEach(): Unit = {
    tempDir = Files.createTempDirectory("dq-sink-test").toFile
    tempFile = new File(tempDir, "results.jsonl")
  }

  override def afterEach(): Unit = {
    if (tempFile.exists()) tempFile.delete()
    if (tempDir.exists()) tempDir.delete()
  }

  describe("FileDataQualitySink") {

    it("should write passed result as JSON line") {
      val sink   = DataQualitySink.file(tempFile.getAbsolutePath)
      val result = DataQualityResult.Passed("row_count", "users", Map("count" -> 100L), timestamp)

      sink.write(result)
      sink.flush()
      sink.close()

      val content = Source.fromFile(tempFile).mkString
      content should include(""""status":"passed"""")
      content should include(""""check_name":"row_count"""")
      content should include(""""table_name":"users"""")
    }

    it("should write failed result with message") {
      val sink   = DataQualitySink.file(tempFile.getAbsolutePath)
      val result = DataQualityResult.Failed("null_check", "orders", "Too many nulls", Map.empty, timestamp)

      sink.write(result)
      sink.flush()
      sink.close()

      val content = Source.fromFile(tempFile).mkString
      content should include(""""status":"failed"""")
      content should include(""""message":"Too many nulls"""")
    }

    it("should write warning result") {
      val sink   = DataQualitySink.file(tempFile.getAbsolutePath)
      val result = DataQualityResult.Warning("threshold_check", "events", "Near limit", Map.empty, timestamp)

      sink.write(result)
      sink.flush()
      sink.close()

      val content = Source.fromFile(tempFile).mkString
      content should include(""""status":"warning"""")
      content should include(""""message":"Near limit"""")
    }

    it("should write skipped result with reason") {
      val sink   = DataQualitySink.file(tempFile.getAbsolutePath)
      val result = DataQualityResult.Skipped("check", "missing", "Table not found", timestamp)

      sink.write(result)
      sink.flush()
      sink.close()

      val content = Source.fromFile(tempFile).mkString
      content should include(""""status":"skipped"""")
      content should include(""""reason":"Table not found"""")
    }

    it("should append multiple results") {
      val sink = DataQualitySink.file(tempFile.getAbsolutePath)
      val r1   = DataQualityResult.Passed("check1", "t1", Map.empty, timestamp)
      val r2   = DataQualityResult.Failed("check2", "t2", "error", Map.empty, timestamp)
      val r3   = DataQualityResult.Passed("check3", "t3", Map.empty, timestamp)

      sink.write(r1)
      sink.write(r2)
      sink.write(r3)
      sink.flush()
      sink.close()

      val lines = Source.fromFile(tempFile).getLines().toList
      lines should have size 3
      lines(0) should include("check1")
      lines(1) should include("check2")
      lines(2) should include("check3")
    }

    it("should use writeAll for multiple results") {
      val sink    = DataQualitySink.file(tempFile.getAbsolutePath)
      val results = Seq(
        DataQualityResult.Passed("c1", "t1", Map.empty, timestamp),
        DataQualityResult.Passed("c2", "t2", Map.empty, timestamp)
      )

      sink.writeAll(results)
      sink.flush()
      sink.close()

      val lines = Source.fromFile(tempFile).getLines().toList
      lines should have size 2
    }
  }

  describe("NoopDataQualitySink") {

    it("should accept writes without error") {
      val sink   = DataQualitySink.noop
      val result = DataQualityResult.Passed("check", "table", Map.empty, timestamp)

      noException should be thrownBy {
        sink.write(result)
        sink.flush()
        sink.close()
      }
    }
  }

  describe("LoggingDataQualitySink") {

    it("should accept writes without error") {
      val sink   = DataQualitySink.logging("info")
      val result = DataQualityResult.Passed("check", "table", Map.empty, timestamp)

      noException should be thrownBy {
        sink.write(result)
        sink.flush()
        sink.close()
      }
    }

    it("should handle different log levels") {
      Seq("info", "debug", "warn").foreach { level =>
        val sink   = DataQualitySink.logging(level)
        val result = DataQualityResult.Passed("check", "table", Map.empty, timestamp)

        noException should be thrownBy {
          sink.write(result)
        }
      }
    }
  }

  describe("CompositeDataQualitySink") {

    it("should write to all sinks") {
      val file1 = new File(tempDir, "results1.jsonl")
      val file2 = new File(tempDir, "results2.jsonl")

      val sink1    = DataQualitySink.file(file1.getAbsolutePath)
      val sink2    = DataQualitySink.file(file2.getAbsolutePath)
      val combined = DataQualitySink.compose(sink1, sink2)

      val result = DataQualityResult.Passed("check", "table", Map.empty, timestamp)
      combined.write(result)
      combined.flush()
      combined.close()

      Source.fromFile(file1).mkString should include("check")
      Source.fromFile(file2).mkString should include("check")

      file1.delete()
      file2.delete()
    }

    it("should handle empty sinks list") {
      val combined = DataQualitySink.compose()
      val result   = DataQualityResult.Passed("check", "table", Map.empty, timestamp)

      noException should be thrownBy {
        combined.write(result)
        combined.flush()
        combined.close()
      }
    }
  }

  describe("DataQualityResultSerializer") {

    describe("toJson") {

      it("should serialize Passed result") {
        val result = DataQualityResult.Passed(
          "row_count",
          "users",
          Map("count" -> 100L),
          timestamp
        )

        val json = DataQualityResultSerializer.toJson(result)

        json should include(""""status":"passed"""")
        json should include(""""check_name":"row_count"""")
        json should include(""""table_name":"users"""")
        json should include(""""timestamp":"2024-01-01T00:00:00Z"""")
      }

      it("should serialize Failed result with message") {
        val result = DataQualityResult.Failed(
          "null_check",
          "orders",
          "Too many nulls",
          Map("null_pct" -> 10.5),
          timestamp
        )

        val json = DataQualityResultSerializer.toJson(result)

        json should include(""""status":"failed"""")
        json should include(""""message":"Too many nulls"""")
      }

      it("should serialize Warning result") {
        val result = DataQualityResult.Warning(
          "threshold",
          "events",
          "Near limit",
          Map("current" -> 95),
          timestamp
        )

        val json = DataQualityResultSerializer.toJson(result)

        json should include(""""status":"warning"""")
        json should include(""""message":"Near limit"""")
      }

      it("should serialize Skipped result") {
        val result = DataQualityResult.Skipped("check", "missing", "Not found", timestamp)

        val json = DataQualityResultSerializer.toJson(result)

        json should include(""""status":"skipped"""")
        json should include(""""reason":"Not found"""")
      }

      it("should escape quotes in messages") {
        val result = DataQualityResult.Failed(
          "check",
          "table",
          """Error with "quotes" inside""",
          Map.empty,
          timestamp
        )

        val json = DataQualityResultSerializer.toJson(result)
        json should include("\\\"quotes\\\"")
      }

      it("should escape newlines in messages") {
        val result = DataQualityResult.Failed(
          "check",
          "table",
          "Line1\nLine2",
          Map.empty,
          timestamp
        )

        val json = DataQualityResultSerializer.toJson(result)
        json should include("Line1\\nLine2")
      }

      it("should handle empty details map") {
        val result = DataQualityResult.Passed("check", "table", Map.empty, timestamp)

        val json = DataQualityResultSerializer.toJson(result)
        json should include(""""details":{}""")
      }

      it("should serialize details with various types") {
        val details: Map[String, Any] = Map(
          "string_val"  -> "text",
          "int_val"     -> 42,
          "double_val"  -> 3.14,
          "boolean_val" -> true
        )
        val result = DataQualityResult.Passed("check", "table", details, timestamp)

        val json = DataQualityResultSerializer.toJson(result)
        json should include(""""string_val":"text"""")
        json should include(""""int_val":42""")
        json should include(""""double_val":3.14""")
        json should include(""""boolean_val":true""")
      }

      it("should handle null values in details") {
        val details: Map[String, Any] = Map("null_val" -> null)
        val result                    = DataQualityResult.Passed("check", "table", details, timestamp)

        val json = DataQualityResultSerializer.toJson(result)
        json should include(""""null_val":null""")
      }
    }
  }
}
