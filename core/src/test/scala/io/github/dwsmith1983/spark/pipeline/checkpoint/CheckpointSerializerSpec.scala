package io.github.dwsmith1983.spark.pipeline.checkpoint

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/** Tests for CheckpointSerializer JSON serialization. */
class CheckpointSerializerSpec extends AnyFunSpec with Matchers {

  val testInstant: Instant = Instant.parse("2024-01-15T10:30:00Z")

  describe("CheckpointSerializer") {

    describe("toJson/fromJson round-trip") {

      it("should serialize and deserialize Running state") {
        val state = CheckpointState(
          runId = "run-123",
          pipelineName = "test-pipeline",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = -1,
          totalComponents = 5,
          status = CheckpointStatus.Running
        )

        val json     = CheckpointSerializer.toJson(state)
        val restored = CheckpointSerializer.fromJson(json)

        restored shouldBe state
      }

      it("should serialize and deserialize Failed state") {
        val state = CheckpointState(
          runId = "run-456",
          pipelineName = "my-etl-pipeline",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = 2,
          totalComponents = 5,
          status = CheckpointStatus.Failed("Connection timeout", 3)
        )

        val json     = CheckpointSerializer.toJson(state)
        val restored = CheckpointSerializer.fromJson(json)

        restored shouldBe state
      }

      it("should serialize and deserialize Completed state") {
        val state = CheckpointState(
          runId = "run-789",
          pipelineName = "daily-batch",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = 4,
          totalComponents = 5,
          status = CheckpointStatus.Completed
        )

        val json     = CheckpointSerializer.toJson(state)
        val restored = CheckpointSerializer.fromJson(json)

        restored shouldBe state
      }

      it("should handle completed components") {
        val components = List(
          ComponentCheckpoint(
            index = 0,
            name = "Component1",
            instanceType = "com.example.Component1",
            startedAt = testInstant,
            completedAt = testInstant.plusMillis(100),
            durationMs = 100L
          ),
          ComponentCheckpoint(
            index = 1,
            name = "Component2",
            instanceType = "com.example.Component2",
            startedAt = testInstant.plusMillis(100),
            completedAt = testInstant.plusMillis(250),
            durationMs = 150L
          )
        )

        val state = CheckpointState(
          runId = "run-abc",
          pipelineName = "multi-step",
          startedAt = testInstant,
          completedComponents = components,
          lastCheckpointedIndex = 1,
          totalComponents = 5,
          status = CheckpointStatus.Running
        )

        val json     = CheckpointSerializer.toJson(state)
        val restored = CheckpointSerializer.fromJson(json)

        restored shouldBe state
        (restored.completedComponents should have).length(2)
        restored.completedComponents.head.name shouldBe "Component1"
        restored.completedComponents(1).durationMs shouldBe 150L
      }

      it("should handle resumedFrom field") {
        val state = CheckpointState(
          runId = "run-resumed",
          pipelineName = "resumable-pipeline",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = 2,
          totalComponents = 5,
          status = CheckpointStatus.Running,
          resumedFrom = Some("run-original")
        )

        val json     = CheckpointSerializer.toJson(state)
        val restored = CheckpointSerializer.fromJson(json)

        restored shouldBe state
        restored.resumedFrom shouldBe Some("run-original")
      }

      it("should handle empty resumedFrom") {
        val state = CheckpointState(
          runId = "run-fresh",
          pipelineName = "fresh-pipeline",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = -1,
          totalComponents = 3,
          status = CheckpointStatus.Running,
          resumedFrom = None
        )

        val json     = CheckpointSerializer.toJson(state)
        val restored = CheckpointSerializer.fromJson(json)

        restored.resumedFrom shouldBe None
      }
    }

    describe("special character handling") {

      it("should escape quotes in pipeline name") {
        val state = CheckpointState(
          runId = "run-123",
          pipelineName = """Pipeline with "quotes"""",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = -1,
          totalComponents = 1,
          status = CheckpointStatus.Running
        )

        val json     = CheckpointSerializer.toJson(state)
        val restored = CheckpointSerializer.fromJson(json)

        restored.pipelineName shouldBe """Pipeline with "quotes""""
      }

      it("should escape newlines in error message") {
        val state = CheckpointState(
          runId = "run-123",
          pipelineName = "test",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = 1,
          totalComponents = 3,
          status = CheckpointStatus.Failed("Error:\nLine 2\nLine 3", 2)
        )

        val json     = CheckpointSerializer.toJson(state)
        val restored = CheckpointSerializer.fromJson(json)

        restored.status match {
          case CheckpointStatus.Failed(msg, _) =>
            msg should include("\n")
          case _ =>
            fail("Expected Failed status")
        }
      }

      it("should handle backslashes") {
        val state = CheckpointState(
          runId = "run-123",
          pipelineName = "path\\to\\pipeline",
          startedAt = testInstant,
          completedComponents = List.empty,
          lastCheckpointedIndex = -1,
          totalComponents = 1,
          status = CheckpointStatus.Running
        )

        val json     = CheckpointSerializer.toJson(state)
        val restored = CheckpointSerializer.fromJson(json)

        restored.pipelineName shouldBe "path\\to\\pipeline"
      }
    }

    describe("error handling") {

      it("should throw CheckpointException for invalid JSON") {
        val invalidJson = "{ invalid json }"

        a[CheckpointException] should be thrownBy {
          CheckpointSerializer.fromJson(invalidJson)
        }
      }

      it("should throw CheckpointException for missing fields") {
        val incompleteJson = """{"runId":"123"}"""

        a[CheckpointException] should be thrownBy {
          CheckpointSerializer.fromJson(incompleteJson)
        }
      }
    }
  }
}
