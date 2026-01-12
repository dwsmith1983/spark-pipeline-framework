package io.github.dwsmith1983.spark.pipeline.streaming

import org.apache.logging.log4j.{LogManager, Logger}

/**
 * Lifecycle hooks for streaming query execution.
 *
 * Implement this trait to add custom behavior at various points during
 * streaming query execution, such as logging, metrics collection, alerting,
 * or custom monitoring.
 *
 * All methods have default no-op implementations, so you only need to
 * override the hooks you're interested in.
 *
 * == Example ==
 *
 * {{{
 * val metricsHooks = new StreamingHooks {
 *   override def onQueryStart(queryName: String, queryId: String): Unit =
 *     metrics.incrementActiveQueries(queryName)
 *
 *   override def onBatchProgress(
 *     queryName: String,
 *     batchId: Long,
 *     numInputRows: Long,
 *     durationMs: Long
 *   ): Unit = {
 *     metrics.recordBatchDuration(queryName, durationMs)
 *     metrics.recordInputRows(queryName, numInputRows)
 *   }
 *
 *   override def onQueryTerminated(
 *     queryName: String,
 *     queryId: String,
 *     exception: Option[Throwable]
 *   ): Unit = {
 *     metrics.decrementActiveQueries(queryName)
 *     exception.foreach(e => alerting.sendAlert(s"Query $$queryName failed: $${e.getMessage}"))
 *   }
 * }
 * }}}
 *
 * @see [[io.github.dwsmith1983.spark.pipeline.config.PipelineHooks]] for batch pipeline hooks
 */
trait StreamingHooks {

  /**
   * Called when a streaming query starts.
   *
   * @param queryName The name of the streaming query
   * @param queryId The unique identifier for this query run
   */
  def onQueryStart(queryName: String, queryId: String): Unit = {
    val _ = (queryName, queryId) // suppress unused warning - meant to be overridden
  }

  /**
   * Called after each micro-batch completes.
   *
   * This hook receives progress information about the completed batch,
   * useful for monitoring throughput and latency.
   *
   * @param queryName The name of the streaming query
   * @param batchId The sequential batch identifier
   * @param numInputRows Number of rows processed in this batch
   * @param durationMs Duration of the batch in milliseconds
   */
  def onBatchProgress(
    queryName: String,
    batchId: Long,
    numInputRows: Long,
    durationMs: Long
  ): Unit = {
    val _ = (queryName, batchId, numInputRows, durationMs) // suppress unused warning
  }

  /**
   * Called when a streaming query terminates.
   *
   * A query can terminate for several reasons:
   * - Normal completion (for queries using Once or AvailableNow triggers)
   * - Manual stop via `query.stop()`
   * - Failure with an exception
   *
   * @param queryName The name of the streaming query
   * @param queryId The unique identifier for this query run
   * @param exception The exception if the query failed, None otherwise
   */
  def onQueryTerminated(
    queryName: String,
    queryId: String,
    exception: Option[Throwable]
  ): Unit = {
    val _ = (queryName, queryId, exception) // suppress unused warning - meant to be overridden
  }
}

/** Companion object providing utilities for working with streaming hooks. */
object StreamingHooks {

  private val logger: Logger = LogManager.getLogger(classOf[StreamingHooks])

  /** A no-op implementation that does nothing at each hook point. */
  val NoOp: StreamingHooks = new StreamingHooks {}

  /**
   * Combines multiple hooks into a single hook that calls each in order.
   *
   * Exceptions thrown by individual hooks are caught and logged,
   * allowing subsequent hooks to still be called. This ensures that a
   * failing logging hook won't prevent metrics collection, for example.
   *
   * @param hooks The hooks to combine
   * @return A composite hook that delegates to all provided hooks
   */
  def compose(hooks: StreamingHooks*): StreamingHooks = new StreamingHooks {

    private def safeCall(hookName: String)(f: StreamingHooks => Unit): Unit =
      hooks.foreach { hook =>
        try {
          f(hook)
        } catch {
          case e: Exception =>
            logger.warn(
              "Hook {}.{} failed: {}",
              hook.getClass.getName: Any,
              hookName: Any,
              e.getMessage: Any
            )
        }
      }

    override def onQueryStart(queryName: String, queryId: String): Unit =
      safeCall("onQueryStart")(_.onQueryStart(queryName, queryId))

    override def onBatchProgress(
      queryName: String,
      batchId: Long,
      numInputRows: Long,
      durationMs: Long
    ): Unit =
      safeCall("onBatchProgress")(_.onBatchProgress(queryName, batchId, numInputRows, durationMs))

    override def onQueryTerminated(
      queryName: String,
      queryId: String,
      exception: Option[Throwable]
    ): Unit =
      safeCall("onQueryTerminated")(_.onQueryTerminated(queryName, queryId, exception))
  }
}
