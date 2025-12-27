package io.github.dwsmith1983.spark.pipeline.audit

/**
 * Trait for audit event sinks.
 *
 * Implementations write audit events to various backends (files, Kafka, webhooks, etc.).
 * Implementations must be thread-safe.
 *
 * == Implementing a Custom Sink ==
 *
 * {{{
 * class WebhookAuditSink(url: String) extends AuditSink {
 *   override def write(event: AuditEvent): Unit = {
 *     val json = AuditEventSerializer.toJson(event)
 *     // POST to webhook URL
 *   }
 *
 *   override def flush(): Unit = ()
 *   override def close(): Unit = ()
 * }
 * }}}
 */
trait AuditSink {

  /**
   * Write a single audit event.
   *
   * Implementations should handle serialization internally.
   * This method should be thread-safe.
   *
   * @param event The audit event to write
   */
  def write(event: AuditEvent): Unit

  /**
   * Flush any buffered events to the underlying storage.
   *
   * Called after pipeline completion to ensure all events are persisted.
   */
  def flush(): Unit

  /**
   * Close the sink and release resources.
   *
   * Called when the audit trail is complete.
   */
  def close(): Unit
}

/**
 * Factory methods for creating AuditSink instances.
 */
object AuditSink {

  /**
   * Creates a file-based sink writing JSON lines to the specified path.
   *
   * @param path Path to the output file (will be created if doesn't exist)
   * @return AuditSink that appends JSON lines to the file
   */
  def file(path: String): AuditSink = new FileAuditSink(path)

  /**
   * Creates a file-based sink with custom options.
   *
   * @param path Path to the output file
   * @param append If true, append to existing file; if false, overwrite
   * @param bufferSize Buffer size in bytes for the writer
   * @return AuditSink with custom configuration
   */
  def file(path: String, append: Boolean, bufferSize: Int): AuditSink =
    new FileAuditSink(path, append, bufferSize)

  /**
   * Creates a composite sink that writes to multiple sinks.
   *
   * Events are written to all sinks. Exceptions from individual sinks
   * are caught and logged, allowing other sinks to continue receiving events.
   *
   * @param sinks The sinks to delegate to
   * @return AuditSink that writes to all provided sinks
   */
  def compose(sinks: AuditSink*): AuditSink = new CompositeAuditSink(sinks.toList)

  /**
   * A no-op sink for testing or when audit is disabled.
   *
   * All operations are no-ops.
   */
  val NoOp: AuditSink = new AuditSink {
    override def write(event: AuditEvent): Unit = ()
    override def flush(): Unit                  = ()
    override def close(): Unit                  = ()
  }
}
