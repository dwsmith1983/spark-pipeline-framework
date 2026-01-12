package io.github.dwsmith1983.spark.pipeline.checkpoint

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Tests for CheckpointConfig. */
class CheckpointConfigSpec extends AnyFunSpec with Matchers {

  describe("CheckpointConfig") {

    describe("defaults") {

      it("should have checkpointing disabled by default") {
        val config = CheckpointConfig()
        config.enabled shouldBe false
      }

      it("should use default location") {
        val config = CheckpointConfig()
        config.location shouldBe FileSystemCheckpointStore.DefaultPath
      }

      it("should have autoResume disabled by default") {
        val config = CheckpointConfig()
        config.autoResume shouldBe false
      }

      it("should have cleanupOnSuccess enabled by default") {
        val config = CheckpointConfig()
        config.cleanupOnSuccess shouldBe true
      }
    }

    describe("createStore") {

      it("should create FileSystemCheckpointStore when enabled") {
        val config = CheckpointConfig(enabled = true, location = "/tmp/test-checkpoints")
        val store  = config.createStore()

        store shouldBe a[FileSystemCheckpointStore]
      }

      it("should return NoOp store when disabled") {
        val config = CheckpointConfig(enabled = false)
        val store  = config.createStore()

        store shouldBe CheckpointStore.NoOp
      }
    }

    describe("createHooks") {

      it("should create CheckpointHooks with config settings") {
        val config = CheckpointConfig(enabled = true, cleanupOnSuccess = false)
        val hooks  = config.createHooks()

        hooks.cleanupOnSuccess shouldBe false
      }

      it("should use provided runId") {
        val config = CheckpointConfig(enabled = true)
        val hooks  = config.createHooks(runId = Some("custom-run-id"))

        hooks.runId shouldBe "custom-run-id"
      }

      it("should track resumedFrom") {
        val config = CheckpointConfig(enabled = true)
        val hooks  = config.createHooks(resumedFrom = Some("original-run"))

        hooks.resumedFrom shouldBe Some("original-run")
      }

      it("should generate runId when not provided") {
        val config = CheckpointConfig(enabled = true)
        val hooks  = config.createHooks()

        hooks.runId should not be empty
        (hooks.runId should have).length(36) // UUID format
      }
    }
  }

  describe("CheckpointConfig factory methods") {

    it("should create default config") {
      val config = CheckpointConfig.default
      config.enabled shouldBe false
    }

    it("should create enabled config") {
      val config = CheckpointConfig.enabled
      config.enabled shouldBe true
    }

    it("should create config at specific location") {
      val config = CheckpointConfig.at("/custom/path")
      config.enabled shouldBe true
      config.location shouldBe "/custom/path"
    }

    it("should create config with auto-resume") {
      val config = CheckpointConfig.withAutoResume("/auto/resume/path")
      config.enabled shouldBe true
      config.autoResume shouldBe true
      config.location shouldBe "/auto/resume/path"
    }
  }
}
