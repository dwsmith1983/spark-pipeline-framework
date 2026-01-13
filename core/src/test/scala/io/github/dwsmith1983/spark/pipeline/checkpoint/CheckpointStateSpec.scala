package io.github.dwsmith1983.spark.pipeline.checkpoint

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/** Tests for CheckpointState and related models. */
class CheckpointStateSpec extends AnyFunSpec with Matchers {

  val testInstant: Instant = Instant.parse("2024-01-15T10:30:00Z")

  describe("CheckpointState") {

    it("should calculate resumeFromIndex as lastCheckpointedIndex + 1") {
      val state = CheckpointState(
        runId = "run-123",
        pipelineName = "test-pipeline",
        startedAt = testInstant,
        completedComponents = List.empty,
        lastCheckpointedIndex = 2,
        totalComponents = 5,
        status = CheckpointStatus.Running
      )

      state.resumeFromIndex shouldBe 3
    }

    it("should return 0 for resumeFromIndex when no components completed") {
      val state = CheckpointState(
        runId = "run-123",
        pipelineName = "test-pipeline",
        startedAt = testInstant,
        completedComponents = List.empty,
        lastCheckpointedIndex = -1,
        totalComponents = 5,
        status = CheckpointStatus.Running
      )

      state.resumeFromIndex shouldBe 0
    }

    describe("canResume") {

      it("should return true for Failed status with incomplete work") {
        val state = CheckpointState(
          runId = "run-123",
          pipelineName = "test-pipeline",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = 2,
          totalComponents = 5,
          status = CheckpointStatus.Failed("Error", 3)
        )

        state.canResume shouldBe true
      }

      it("should return false for Failed status when all components except last completed") {
        val state = CheckpointState(
          runId = "run-123",
          pipelineName = "test-pipeline",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = 4,
          totalComponents = 5,
          status = CheckpointStatus.Failed("Error", 4)
        )

        // lastCheckpointedIndex (4) is not < totalComponents - 1 (4)
        state.canResume shouldBe false
      }

      it("should return false for Running status") {
        val state = CheckpointState(
          runId = "run-123",
          pipelineName = "test-pipeline",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = 2,
          totalComponents = 5,
          status = CheckpointStatus.Running
        )

        state.canResume shouldBe false
      }

      it("should return false for Completed status") {
        val state = CheckpointState(
          runId = "run-123",
          pipelineName = "test-pipeline",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = 4,
          totalComponents = 5,
          status = CheckpointStatus.Completed
        )

        state.canResume shouldBe false
      }
    }

    it("should track resumedFrom lineage") {
      val state = CheckpointState(
        runId = "run-456",
        pipelineName = "test-pipeline",
        startedAt = testInstant,
        completedComponents = List.empty,
        lastCheckpointedIndex = 2,
        totalComponents = 5,
        status = CheckpointStatus.Running,
        resumedFrom = Some("run-123")
      )

      state.resumedFrom shouldBe Some("run-123")
    }
  }

  describe("ComponentCheckpoint") {

    it("should store component execution details") {
      val checkpoint = ComponentCheckpoint(
        index = 0,
        name = "MyComponent",
        instanceType = "com.example.MyComponent",
        startedAt = testInstant,
        completedAt = testInstant.plusMillis(100),
        durationMs = 100L
      )

      checkpoint.index shouldBe 0
      checkpoint.name shouldBe "MyComponent"
      checkpoint.instanceType shouldBe "com.example.MyComponent"
      checkpoint.durationMs shouldBe 100L
    }
  }

  describe("CheckpointStatus") {

    it("should have Running status") {
      CheckpointStatus.Running shouldBe a[CheckpointStatus]
    }

    it("should have Completed status") {
      CheckpointStatus.Completed shouldBe a[CheckpointStatus]
    }

    it("should have Failed status with details") {
      val failed = CheckpointStatus.Failed("Connection timeout", 2)

      failed.errorMessage shouldBe "Connection timeout"
      failed.failedIndex shouldBe 2
    }
  }
}
