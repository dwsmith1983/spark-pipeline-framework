package io.github.dwsmith1983.spark.pipeline.checkpoint

import com.typesafe.config.ConfigFactory
import io.github.dwsmith1983.spark.pipeline.config._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import scala.collection.mutable.ListBuffer

/** In-memory checkpoint store for testing. */
class TestCheckpointStore extends CheckpointStore {
  val saved: ListBuffer[CheckpointState] = ListBuffer.empty
  val deleted: ListBuffer[String]        = ListBuffer.empty
  var saveCount: Int                     = 0

  override def save(state: CheckpointState): Unit = {
    saveCount += 1
    // Remove old version if exists (Scala 2.12 compatible)
    val toRemove = saved.filter(_.runId == state.runId)
    saved --= toRemove
    saved += state
  }

  override def loadLatest(pipelineName: String): Option[CheckpointState] =
    saved
      .filter(s => s.pipelineName == pipelineName && s.status.isInstanceOf[CheckpointStatus.Failed])
      .sortBy(_.startedAt)(Ordering[java.time.Instant].reverse)
      .headOption

  override def loadByRunId(runId: String): Option[CheckpointState] =
    saved.find(_.runId == runId)

  override def delete(runId: String): Unit = {
    deleted += runId
    // Scala 2.12 compatible removal
    val toRemove = saved.filter(_.runId == runId)
    saved --= toRemove
  }

  override def listIncomplete(): List[CheckpointState] =
    saved.filter(_.status.isInstanceOf[CheckpointStatus.Failed]).toList

  override def listIncomplete(pipelineName: String): List[CheckpointState] =
    saved.filter(s => s.pipelineName == pipelineName && s.status.isInstanceOf[CheckpointStatus.Failed]).toList

  def clear(): Unit = {
    saved.clear()
    deleted.clear()
    saveCount = 0
  }

  def latestState: Option[CheckpointState] = saved.lastOption
}

/** Tests for CheckpointHooks. */
class CheckpointHooksSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  var store: TestCheckpointStore = _
  var hooks: CheckpointHooks     = _

  val testPipelineConfig: PipelineConfig = PipelineConfig(
    pipelineName = "Test Pipeline",
    pipelineComponents = List(
      ComponentConfig(
        instanceType = "com.example.Component1",
        instanceName = "Component1",
        instanceConfig = ConfigFactory.empty()
      ),
      ComponentConfig(
        instanceType = "com.example.Component2",
        instanceName = "Component2",
        instanceConfig = ConfigFactory.empty()
      ),
      ComponentConfig(
        instanceType = "com.example.Component3",
        instanceName = "Component3",
        instanceConfig = ConfigFactory.empty()
      )
    )
  )

  override def beforeEach(): Unit = {
    store = new TestCheckpointStore()
    hooks = new CheckpointHooks(store, runId = "test-run-123")
  }

  describe("CheckpointHooks") {

    describe("beforePipeline") {

      it("should initialize checkpoint state") {
        hooks.beforePipeline(testPipelineConfig)

        store.latestState shouldBe defined
        val state = store.latestState.get
        state.runId shouldBe "test-run-123"
        state.pipelineName shouldBe "Test Pipeline"
        state.totalComponents shouldBe 3
        state.lastCheckpointedIndex shouldBe -1
        state.status shouldBe CheckpointStatus.Running
      }

      it("should save initial checkpoint to store") {
        hooks.beforePipeline(testPipelineConfig)

        (store.saved should have).length(1)
      }
    }

    describe("afterComponent") {

      it("should update checkpoint after each component") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testPipelineConfig.pipelineComponents.head, 0, 3)
        hooks.afterComponent(testPipelineConfig.pipelineComponents.head, 0, 3, 100L)

        val state = store.latestState.get
        state.lastCheckpointedIndex shouldBe 0
        (state.completedComponents should have).length(1)
      }

      it("should accumulate completed components") {
        hooks.beforePipeline(testPipelineConfig)

        testPipelineConfig.pipelineComponents.zipWithIndex.foreach {
          case (comp, idx) =>
            hooks.beforeComponent(comp, idx, 3)
            hooks.afterComponent(comp, idx, 3, 100L)
        }

        val state = store.latestState.get
        state.lastCheckpointedIndex shouldBe 2
        (state.completedComponents should have).length(3)
      }

      it("should record component details") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testPipelineConfig.pipelineComponents.head, 0, 3)
        hooks.afterComponent(testPipelineConfig.pipelineComponents.head, 0, 3, 150L)

        val checkpoint = store.latestState.get.completedComponents.head
        checkpoint.index shouldBe 0
        checkpoint.name shouldBe "Component1"
        checkpoint.instanceType shouldBe "com.example.Component1"
        checkpoint.durationMs shouldBe 150L
      }

      it("should save to store after each component") {
        hooks.beforePipeline(testPipelineConfig)
        val initialSaveCount = store.saveCount

        hooks.beforeComponent(testPipelineConfig.pipelineComponents.head, 0, 3)
        hooks.afterComponent(testPipelineConfig.pipelineComponents.head, 0, 3, 100L)

        store.saveCount shouldBe (initialSaveCount + 1)

        hooks.beforeComponent(testPipelineConfig.pipelineComponents(1), 1, 3)
        hooks.afterComponent(testPipelineConfig.pipelineComponents(1), 1, 3, 100L)

        store.saveCount shouldBe (initialSaveCount + 2)
      }
    }

    describe("onComponentFailure") {

      it("should update status to Failed") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testPipelineConfig.pipelineComponents.head, 0, 3)
        hooks.onComponentFailure(testPipelineConfig.pipelineComponents.head, 0, new RuntimeException("Test error"))

        val state = store.latestState.get
        state.status shouldBe a[CheckpointStatus.Failed]
        state.status.asInstanceOf[CheckpointStatus.Failed].errorMessage shouldBe "Test error"
        state.status.asInstanceOf[CheckpointStatus.Failed].failedIndex shouldBe 0
      }

      it("should save failed state to store") {
        hooks.beforePipeline(testPipelineConfig)
        store.saved.clear()

        hooks.beforeComponent(testPipelineConfig.pipelineComponents.head, 0, 3)
        hooks.onComponentFailure(testPipelineConfig.pipelineComponents.head, 0, new RuntimeException("Error"))

        (store.saved should have).length(1)
        store.latestState.get.status shouldBe a[CheckpointStatus.Failed]
      }

      it("should handle null error message") {
        hooks.beforePipeline(testPipelineConfig)
        hooks.beforeComponent(testPipelineConfig.pipelineComponents.head, 0, 3)
        hooks.onComponentFailure(testPipelineConfig.pipelineComponents.head, 0, new RuntimeException(null: String))

        val state = store.latestState.get
        state.status.asInstanceOf[CheckpointStatus.Failed].errorMessage shouldBe "RuntimeException"
      }
    }

    describe("afterPipeline") {

      describe("on success") {

        it("should delete checkpoint when cleanupOnSuccess is true") {
          val cleanupHooks = new CheckpointHooks(store, runId = "cleanup-run", cleanupOnSuccess = true)

          cleanupHooks.beforePipeline(testPipelineConfig)
          cleanupHooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 3))

          store.deleted should contain("cleanup-run")
        }

        it("should keep checkpoint when cleanupOnSuccess is false") {
          val keepHooks = new CheckpointHooks(store, runId = "keep-run", cleanupOnSuccess = false)

          keepHooks.beforePipeline(testPipelineConfig)
          keepHooks.afterPipeline(testPipelineConfig, PipelineResult.Success(100L, 3))

          store.deleted should not contain "keep-run"
          store.latestState.get.status shouldBe CheckpointStatus.Completed
        }
      }

      describe("on failure") {

        it("should keep checkpoint for resume") {
          hooks.beforePipeline(testPipelineConfig)
          hooks.beforeComponent(testPipelineConfig.pipelineComponents.head, 0, 3)
          hooks.onComponentFailure(testPipelineConfig.pipelineComponents.head, 0, new RuntimeException("Error"))
          hooks.afterPipeline(
            testPipelineConfig,
            PipelineResult.Failure(
              new RuntimeException("Error"),
              Some(testPipelineConfig.pipelineComponents.head),
              0
            )
          )

          store.deleted shouldBe empty
          store.latestState shouldBe defined
        }
      }

      describe("on partial success") {

        it("should mark as failed with first failure info") {
          hooks.beforePipeline(testPipelineConfig)
          hooks.afterPipeline(
            testPipelineConfig,
            PipelineResult.PartialSuccess(
              200L,
              2,
              1,
              List((testPipelineConfig.pipelineComponents(2), new RuntimeException("Partial error")))
            )
          )

          val state = store.latestState.get
          state.status shouldBe a[CheckpointStatus.Failed]
        }
      }
    }

    describe("getState") {

      it("should return current state") {
        hooks.beforePipeline(testPipelineConfig)

        val state = hooks.getState
        state shouldBe defined
        state.get.runId shouldBe "test-run-123"
      }

      it("should return None before beforePipeline") {
        hooks.getState shouldBe None
      }
    }

    describe("resumedFrom tracking") {

      it("should track original run when resuming") {
        val resumeHooks = CheckpointHooks.forResume(store, "original-run")

        resumeHooks.beforePipeline(testPipelineConfig)

        val state = store.latestState.get
        state.resumedFrom shouldBe Some("original-run")
      }
    }
  }

  describe("CheckpointHooks factory methods") {

    it("should create hooks with store") {
      val h = CheckpointHooks(store)
      h.store shouldBe store
    }

    it("should create hooks for resume") {
      val h = CheckpointHooks.forResume(store, "original-123")
      h.resumedFrom shouldBe Some("original-123")
    }
  }
}
