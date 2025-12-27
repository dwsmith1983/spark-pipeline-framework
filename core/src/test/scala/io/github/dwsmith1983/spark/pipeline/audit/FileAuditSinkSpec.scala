package io.github.dwsmith1983.spark.pipeline.audit

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{Files, Path}
import java.time.Instant
import scala.io.Source

/** Tests for FileAuditSink. */
class FileAuditSinkSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  var tempDir: Path = _
  var testFilePath: String = _

  val testContext: SystemContext = SystemContext(
    hostname = "test-host",
    jvmVersion = "17",
    scalaVersion = "2.13.12"
  )

  val testEvent: PipelineStartEvent = PipelineStartEvent(
    eventId = "event-123",
    runId = "run-456",
    timestamp = Instant.parse("2024-01-01T00:00:00Z"),
    pipelineName = "Test Pipeline",
    componentCount = 2,
    failFast = true,
    systemContext = testContext,
    sparkContext = None
  )

  override def beforeEach(): Unit = {
    tempDir = Files.createTempDirectory("audit-sink-test")
    testFilePath = tempDir.resolve("audit.jsonl").toString
  }

  override def afterEach(): Unit = {
    // Clean up temp files
    Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete)
  }

  describe("FileAuditSink") {

    describe("write") {

      it("should create file and write JSON line") {
        val sink = new FileAuditSink(testFilePath)
        sink.write(testEvent)
        sink.flush()
        sink.close()

        val content = Source.fromFile(testFilePath).mkString
        content should include("event_id")
        content should include("event-123")
        content should include("pipeline_start")
      }

      it("should write one event per line") {
        val sink = new FileAuditSink(testFilePath)
        sink.write(testEvent)
        sink.write(testEvent.copy(eventId = "event-456"))
        sink.flush()
        sink.close()

        val lines = Source.fromFile(testFilePath).getLines().toList
        lines should have length 2
      }

      it("should create parent directories if they don't exist") {
        val nestedPath = tempDir.resolve("nested/dir/audit.jsonl").toString
        val sink = new FileAuditSink(nestedPath)
        sink.write(testEvent)
        sink.flush()
        sink.close()

        Files.exists(Path.of(nestedPath)) shouldBe true
      }

      it("should append to existing file when append=true") {
        // Write first line
        val sink1 = new FileAuditSink(testFilePath, append = true)
        sink1.write(testEvent)
        sink1.close()

        // Write second line
        val sink2 = new FileAuditSink(testFilePath, append = true)
        sink2.write(testEvent.copy(eventId = "event-456"))
        sink2.close()

        val lines = Source.fromFile(testFilePath).getLines().toList
        lines should have length 2
      }

      it("should overwrite existing file when append=false") {
        // Write first line
        val sink1 = new FileAuditSink(testFilePath, append = true)
        sink1.write(testEvent)
        sink1.close()

        // Overwrite
        val sink2 = new FileAuditSink(testFilePath, append = false)
        sink2.write(testEvent.copy(eventId = "event-456"))
        sink2.close()

        val lines = Source.fromFile(testFilePath).getLines().toList
        lines should have length 1
        lines.head should include("event-456")
      }
    }

    describe("flush") {

      it("should flush buffered content to disk") {
        val sink = new FileAuditSink(testFilePath)
        sink.write(testEvent)
        sink.flush()

        val content = Source.fromFile(testFilePath).mkString
        content should not be empty
        sink.close()
      }
    }

    describe("close") {

      it("should flush buffered content before closing") {
        val sink = new FileAuditSink(testFilePath)
        sink.write(testEvent)
        sink.close()

        // Content should be written even without explicit flush
        val content = Source.fromFile(testFilePath).mkString
        content should include("event-123")
      }
    }

    describe("thread safety") {

      it("should handle concurrent writes") {
        val sink = new FileAuditSink(testFilePath)
        val threads = (1 to 10).map { i =>
          new Thread(() => {
            sink.write(testEvent.copy(eventId = s"event-$i"))
          })
        }

        threads.foreach(_.start())
        threads.foreach(_.join())
        sink.flush()
        sink.close()

        val lines = Source.fromFile(testFilePath).getLines().toList
        lines should have length 10
      }
    }

    describe("error handling") {

      it("should handle write after close gracefully (no exception)") {
        val sink = new FileAuditSink(testFilePath)
        sink.write(testEvent)
        sink.close()

        // Write after close should not throw, just be ignored
        noException should be thrownBy {
          sink.write(testEvent.copy(eventId = "ignored"))
        }

        // Verify only the first event was written
        val lines = Source.fromFile(testFilePath).getLines().toList
        lines should have length 1
        lines.head should include("event-123")
        lines.head should not include "ignored"
      }

      it("should handle flush after close gracefully") {
        val sink = new FileAuditSink(testFilePath)
        sink.write(testEvent)
        sink.close()

        // Flush after close should not throw
        noException should be thrownBy {
          sink.flush()
        }
      }

      it("should handle multiple close calls gracefully") {
        val sink = new FileAuditSink(testFilePath)
        sink.write(testEvent)
        sink.close()

        // Multiple close calls should not throw
        noException should be thrownBy {
          sink.close()
          sink.close()
        }
      }

      it("should handle I/O errors gracefully during write") {
        // Create a file, then make parent directory read-only
        val readOnlyDir  = tempDir.resolve("readonly")
        Files.createDirectory(readOnlyDir)
        val readOnlyFile = readOnlyDir.resolve("audit.jsonl").toString

        val sink = new FileAuditSink(readOnlyFile)
        sink.write(testEvent)
        sink.close()

        // Make directory read-only to cause I/O error on next write
        readOnlyDir.toFile.setWritable(false)

        try {
          val sink2 = new FileAuditSink(readOnlyFile, append = true)
          // This should not throw, just log error to stderr
          noException should be thrownBy {
            sink2.write(testEvent.copy(eventId = "should-fail"))
          }
          sink2.close()
        } finally {
          // Restore permissions for cleanup
          val _ = readOnlyDir.toFile.setWritable(true)
        }
      }

      it("should create parent directories that don't exist") {
        val deepPath = tempDir.resolve("a/b/c/d/audit.jsonl").toString
        val sink     = new FileAuditSink(deepPath)
        sink.write(testEvent)
        sink.flush()
        sink.close()

        Files.exists(Path.of(deepPath)) shouldBe true
        val content = Source.fromFile(deepPath).mkString
        content should include("event-123")
      }
    }
  }

  describe("AuditSink factory methods") {

    it("should create file sink with file()") {
      val sink = AuditSink.file(testFilePath)
      sink shouldBe a[FileAuditSink]
      sink.close()
    }

    it("should create file sink with custom options") {
      val sink = AuditSink.file(testFilePath, append = false, bufferSize = 4096)
      sink shouldBe a[FileAuditSink]
      sink.asInstanceOf[FileAuditSink].append shouldBe false
      sink.asInstanceOf[FileAuditSink].bufferSize shouldBe 4096
      sink.close()
    }

    it("should return NoOp sink") {
      val sink = AuditSink.NoOp
      sink.write(testEvent) // Should not throw
      sink.flush()
      sink.close()
    }
  }
}
