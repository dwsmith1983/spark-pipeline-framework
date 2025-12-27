package io.github.dwsmith1983.spark.pipeline.audit

import org.apache.logging.log4j.{LogManager, Logger}

import java.io.{BufferedWriter, FileWriter, IOException}
import java.nio.file.{Files, Paths}

/**
 * Audit sink that writes JSON lines to a file.
 *
 * Events are appended one per line in JSON format (JSON Lines / NDJSON format)
 * for easy parsing and streaming.
 *
 * Thread-safe via synchronization on write operations.
 *
 * == Example Output ==
 *
 * {{{
 * {"event_id":"abc123","event_type":"pipeline_start","run_id":"xyz789",...}
 * {"event_id":"def456","event_type":"component_start","run_id":"xyz789",...}
 * {"event_id":"ghi789","event_type":"component_end","run_id":"xyz789",...}
 * }}}
 *
 * @param path Path to the output file
 * @param append If true (default), append to existing file; if false, overwrite
 * @param bufferSize Buffer size in bytes for the writer (default 8KB)
 */
class FileAuditSink(
  val path: String,
  val append: Boolean = true,
  val bufferSize: Int = 8192)
  extends AuditSink {

  private val logger: Logger                   = LogManager.getLogger(classOf[FileAuditSink])
  private val lock                             = new Object
  @volatile private var writer: BufferedWriter = _
  @volatile private var closed: Boolean        = false
  @volatile private var initialized: Boolean   = false

  private def ensureWriter(): BufferedWriter = {
    if (!initialized) {
      lock.synchronized {
        if (!initialized && !closed) {
          val pathObj = Paths.get(path)
          val parent  = pathObj.getParent
          if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent)
          }
          writer = new BufferedWriter(new FileWriter(path, append), bufferSize)
          initialized = true
        }
      }
    }
    writer
  }

  override def write(event: AuditEvent): Unit = lock.synchronized {
    if (!closed) {
      try {
        val json = AuditEventSerializer.toJson(event)
        val w    = ensureWriter()
        if (w != null) {
          w.write(json)
          w.newLine()
        }
      } catch {
        case e: IOException =>
          logger.warn("Failed to write audit event: {}", e.getMessage)
      }
    }
  }

  override def flush(): Unit = lock.synchronized {
    if (writer != null && !closed) {
      try {
        writer.flush()
      } catch {
        case e: IOException =>
          logger.warn("Failed to flush audit sink: {}", e.getMessage)
      }
    }
  }

  override def close(): Unit = lock.synchronized {
    if (!closed) {
      closed = true
      if (writer != null) {
        try {
          writer.close()
        } catch {
          case _: IOException => ()
        }
        writer = null
      }
    }
  }
}
