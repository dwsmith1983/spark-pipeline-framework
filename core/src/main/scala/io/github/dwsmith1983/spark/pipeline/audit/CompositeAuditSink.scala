package io.github.dwsmith1983.spark.pipeline.audit

import org.apache.logging.log4j.{LogManager, Logger}

/**
 * Audit sink that delegates to multiple underlying sinks.
 *
 * Exceptions from individual sinks are caught and logged,
 * allowing other sinks to continue receiving events. This ensures
 * that a failing sink (e.g., network issues with a webhook) doesn't
 * prevent audit events from being written to other sinks (e.g., local file).
 *
 * @param sinks List of sinks to delegate to
 */
class CompositeAuditSink(sinks: List[AuditSink]) extends AuditSink {

  private val logger: Logger = LogManager.getLogger(classOf[CompositeAuditSink])

  override def write(event: AuditEvent): Unit =
    sinks.foreach { sink =>
      try {
        sink.write(event)
      } catch {
        case e: Exception =>
          logger.warn("Sink {} failed to write: {}", sink.getClass.getName: Any, e.getMessage: Any)
      }
    }

  override def flush(): Unit =
    sinks.foreach { sink =>
      try {
        sink.flush()
      } catch {
        case e: Exception =>
          logger.warn("Sink {} failed to flush: {}", sink.getClass.getName: Any, e.getMessage: Any)
      }
    }

  override def close(): Unit =
    sinks.foreach { sink =>
      try {
        sink.close()
      } catch {
        case e: Exception =>
          logger.warn("Sink {} failed to close: {}", sink.getClass.getName: Any, e.getMessage: Any)
      }
    }
}
