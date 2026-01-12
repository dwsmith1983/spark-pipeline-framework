package io.github.dwsmith1983.spark.pipeline.streaming

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

/** Tests for StreamingHooks trait and companion object. */
class StreamingHooksSpec extends AnyFunSpec with Matchers {

  describe("StreamingHooks") {

    describe("default implementations") {

      it("should have no-op defaults for all methods") {
        val hooks = new StreamingHooks {}

        // Should not throw
        noException should be thrownBy {
          hooks.onQueryStart("test-query", "query-123")
          hooks.onBatchProgress("test-query", 1L, 100L, 500L)
          hooks.onQueryTerminated("test-query", "query-123", None)
          hooks.onQueryTerminated("test-query", "query-123", Some(new RuntimeException("test")))
        }
      }
    }

    describe("NoOp instance") {

      it("should be a singleton") {
        StreamingHooks.NoOp shouldBe StreamingHooks.NoOp
      }

      it("should have no-op behavior") {
        noException should be thrownBy {
          StreamingHooks.NoOp.onQueryStart("q", "id")
          StreamingHooks.NoOp.onBatchProgress("q", 0L, 0L, 0L)
          StreamingHooks.NoOp.onQueryTerminated("q", "id", None)
        }
      }
    }

    describe("compose") {

      it("should call all hooks in order") {
        val calls = ArrayBuffer.empty[String]

        val hook1 = new StreamingHooks {
          override def onQueryStart(queryName: String, queryId: String): Unit =
            calls += s"hook1:start:$queryName"
        }

        val hook2 = new StreamingHooks {
          override def onQueryStart(queryName: String, queryId: String): Unit =
            calls += s"hook2:start:$queryName"
        }

        val composed = StreamingHooks.compose(hook1, hook2)
        composed.onQueryStart("test", "id-123")

        calls should contain inOrderOnly ("hook1:start:test", "hook2:start:test")
      }

      it("should continue calling hooks even if one throws") {
        val calls = ArrayBuffer.empty[String]

        val failingHook = new StreamingHooks {
          override def onQueryStart(queryName: String, queryId: String): Unit =
            throw new RuntimeException("hook failed")
        }

        val successHook = new StreamingHooks {
          override def onQueryStart(queryName: String, queryId: String): Unit =
            calls += "success"
        }

        val composed = StreamingHooks.compose(failingHook, successHook)

        // Should not throw
        noException should be thrownBy {
          composed.onQueryStart("test", "id")
        }

        // Second hook should still be called
        calls should contain("success")
      }

      it("should compose onBatchProgress") {
        val batches = ArrayBuffer.empty[(String, Long, Long)]

        val hook = new StreamingHooks {
          override def onBatchProgress(
            queryName: String,
            batchId: Long,
            numInputRows: Long,
            durationMs: Long
          ): Unit =
            batches += ((queryName, batchId, numInputRows))
        }

        val composed = StreamingHooks.compose(hook)
        composed.onBatchProgress("query1", 5L, 1000L, 250L)

        batches should contain(("query1", 5L, 1000L))
      }

      it("should compose onQueryTerminated") {
        val terminations = ArrayBuffer.empty[(String, Option[Throwable])]

        val hook = new StreamingHooks {
          override def onQueryTerminated(
            queryName: String,
            queryId: String,
            exception: Option[Throwable]
          ): Unit =
            terminations += ((queryName, exception))
        }

        val composed = StreamingHooks.compose(hook)
        val error    = new RuntimeException("test error")

        composed.onQueryTerminated("query1", "id1", None)
        composed.onQueryTerminated("query2", "id2", Some(error))

        terminations should have size 2
        terminations(0) shouldBe (("query1", None))
        terminations(1)._1 shouldBe "query2"
        terminations(1)._2 shouldBe defined
      }

      it("should handle empty hooks list") {
        val composed = StreamingHooks.compose()

        noException should be thrownBy {
          composed.onQueryStart("q", "id")
          composed.onBatchProgress("q", 0L, 0L, 0L)
          composed.onQueryTerminated("q", "id", None)
        }
      }
    }
  }
}
