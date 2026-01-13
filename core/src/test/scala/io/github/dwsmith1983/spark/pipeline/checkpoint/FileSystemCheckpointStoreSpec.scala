package io.github.dwsmith1983.spark.pipeline.checkpoint

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{Files, Path}
import java.time.Instant

/** Tests for FileSystemCheckpointStore. */
class FileSystemCheckpointStoreSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  var tempDir: Path                    = _
  var store: FileSystemCheckpointStore = _
  val testInstant: Instant             = Instant.parse("2024-01-15T10:30:00Z")

  override def beforeEach(): Unit = {
    tempDir = Files.createTempDirectory("checkpoint-test")
    store = FileSystemCheckpointStore(tempDir.toString)
  }

  override def afterEach(): Unit =
    // Clean up temp directory
    if (Files.exists(tempDir)) {
      deleteRecursively(tempDir)
    }

  private def deleteRecursively(path: Path): Unit = {
    if (Files.isDirectory(path)) {
      val stream = Files.list(path)
      try {
        stream.forEach(child => deleteRecursively(child))
      } finally {
        stream.close()
      }
    }
    Files.deleteIfExists(path)
  }

  def createTestState(
    runId: String = "run-123",
    pipelineName: String = "test-pipeline",
    lastIndex: Int = -1,
    status: CheckpointStatus = CheckpointStatus.Running
  ): CheckpointState =
    CheckpointState(
      runId = runId,
      pipelineName = pipelineName,
      startedAt = testInstant,
      completedComponents = List.empty,
      lastCheckpointedIndex = lastIndex,
      totalComponents = 5,
      status = status
    )

  describe("FileSystemCheckpointStore") {

    describe("save and load") {

      it("should save and load checkpoint by runId") {
        val state = createTestState()

        store.save(state)
        val loaded = store.loadByRunId("run-123")

        loaded shouldBe defined
        loaded.get shouldBe state
      }

      it("should overwrite existing checkpoint with same runId") {
        val state1 = createTestState(lastIndex = 0)
        val state2 = createTestState(lastIndex = 2)

        store.save(state1)
        store.save(state2)

        val loaded = store.loadByRunId("run-123")
        loaded.get.lastCheckpointedIndex shouldBe 2
      }

      it("should return None for non-existent runId") {
        val loaded = store.loadByRunId("non-existent")
        loaded shouldBe None
      }

      it("should store checkpoints in pipeline-specific directories") {
        val state = createTestState()
        store.save(state)

        val pipelineDir = tempDir.resolve("test-pipeline")
        Files.exists(pipelineDir) shouldBe true
        Files.exists(pipelineDir.resolve("run-123.json")) shouldBe true
      }
    }

    describe("loadLatest") {

      it("should return the most recent failed checkpoint") {
        val older = createTestState(
          runId = "run-old",
          status = CheckpointStatus.Failed("Error 1", 1)
        ).copy(startedAt = testInstant.minusSeconds(100))

        val newer = createTestState(
          runId = "run-new",
          status = CheckpointStatus.Failed("Error 2", 2)
        ).copy(startedAt = testInstant)

        store.save(older)
        store.save(newer)

        val latest = store.loadLatest("test-pipeline")
        latest shouldBe defined
        latest.get.runId shouldBe "run-new"
      }

      it("should only return Failed checkpoints") {
        val running   = createTestState(runId = "run-1", status = CheckpointStatus.Running)
        val completed = createTestState(runId = "run-2", status = CheckpointStatus.Completed)
        val failed    = createTestState(runId = "run-3", status = CheckpointStatus.Failed("Error", 1))

        store.save(running)
        store.save(completed)
        store.save(failed)

        val latest = store.loadLatest("test-pipeline")
        latest shouldBe defined
        latest.get.runId shouldBe "run-3"
      }

      it("should return None when no failed checkpoints exist") {
        val running = createTestState(status = CheckpointStatus.Running)
        store.save(running)

        val latest = store.loadLatest("test-pipeline")
        latest shouldBe None
      }

      it("should return None for non-existent pipeline") {
        val latest = store.loadLatest("non-existent-pipeline")
        latest shouldBe None
      }
    }

    describe("delete") {

      it("should delete checkpoint by runId") {
        val state = createTestState()
        store.save(state)

        store.loadByRunId("run-123") shouldBe defined

        store.delete("run-123")

        store.loadByRunId("run-123") shouldBe None
      }

      it("should not throw when deleting non-existent checkpoint") {
        noException should be thrownBy {
          store.delete("non-existent")
        }
      }
    }

    describe("listIncomplete") {

      it("should list all incomplete checkpoints") {
        val failed1 = createTestState(
          runId = "run-1",
          pipelineName = "pipeline-a",
          status = CheckpointStatus.Failed("Error", 1)
        )
        val failed2 = createTestState(
          runId = "run-2",
          pipelineName = "pipeline-b",
          status = CheckpointStatus.Failed("Error", 2)
        )
        val running = createTestState(
          runId = "run-3",
          pipelineName = "pipeline-a",
          status = CheckpointStatus.Running
        )

        store.save(failed1)
        store.save(failed2)
        store.save(running)

        val incomplete = store.listIncomplete()
        (incomplete should have).length(2)
        (incomplete.map(_.runId) should contain).allOf("run-1", "run-2")
      }

      it("should list incomplete checkpoints for specific pipeline") {
        val failed1 = createTestState(
          runId = "run-1",
          pipelineName = "pipeline-a",
          status = CheckpointStatus.Failed("Error", 1)
        )
        val failed2 = createTestState(
          runId = "run-2",
          pipelineName = "pipeline-b",
          status = CheckpointStatus.Failed("Error", 2)
        )

        store.save(failed1)
        store.save(failed2)

        val incomplete = store.listIncomplete("pipeline-a")
        (incomplete should have).length(1)
        incomplete.head.runId shouldBe "run-1"
      }

      it("should return empty list when no incomplete checkpoints") {
        val completed = createTestState(status = CheckpointStatus.Completed)
        store.save(completed)

        val incomplete = store.listIncomplete()
        incomplete shouldBe empty
      }
    }

    describe("pipeline name sanitization") {

      it("should sanitize pipeline names with special characters") {
        val state = createTestState(pipelineName = "My Pipeline (prod)/v2")
        store.save(state)

        val loaded = store.loadLatest("My Pipeline (prod)/v2")
        loaded shouldBe None // Sanitized name won't match

        // The file should exist with sanitized name
        val sanitizedDir = tempDir.resolve("My_Pipeline__prod__v2")
        Files.exists(sanitizedDir) shouldBe true
      }

      it("should handle spaces in pipeline names") {
        val state = createTestState(pipelineName = "My Data Pipeline")
        store.save(state)

        val sanitizedDir = tempDir.resolve("My_Data_Pipeline")
        Files.exists(sanitizedDir) shouldBe true
      }
    }

    describe("atomic writes") {

      it("should not leave temp files on successful save") {
        val state = createTestState()
        store.save(state)

        val pipelineDir = tempDir.resolve("test-pipeline")
        val stream      = Files.list(pipelineDir)
        try {
          stream.forEach(f => (f.getFileName.toString should not).startWith("."))
        } finally {
          stream.close()
        }
      }
    }
  }

  describe("CheckpointStore.NoOp") {

    it("should not persist anything") {
      val noOp  = CheckpointStore.NoOp
      val state = createTestState()

      noOp.save(state)
      noOp.loadByRunId("run-123") shouldBe None
      noOp.loadLatest("test-pipeline") shouldBe None
      noOp.listIncomplete() shouldBe empty
    }
  }

  describe("CheckpointException") {

    it("should create save failure exception") {
      val ex = CheckpointException.saveFailure("run-123", new RuntimeException("IO Error"))
      ex.getMessage should include("run-123")
      ex.getCause shouldBe a[RuntimeException]
    }

    it("should create config mismatch exception") {
      val ex = CheckpointException.configMismatch(5, 3)
      ex.getMessage should include("5")
      ex.getMessage should include("3")
    }
  }
}
