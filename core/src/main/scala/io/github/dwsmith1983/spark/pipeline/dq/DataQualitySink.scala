package io.github.dwsmith1983.spark.pipeline.dq

import org.apache.logging.log4j.{LogManager, Logger}

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.time.format.DateTimeFormatter

/**
 * Sink for writing data quality check results.
 *
 * Similar to AuditSink, this trait defines where and how data quality
 * results are persisted or reported.
 *
 * == Example ==
 *
 * {{{
 * val sink = DataQualitySink.file("/var/log/dq-results.jsonl")
 * sink.write(result)
 * sink.flush()
 * sink.close()
 * }}}
 */
trait DataQualitySink {

  /** Write a single data quality result. */
  def write(result: DataQualityResult): Unit

  /** Write multiple results at once. */
  def writeAll(results: Seq[DataQualityResult]): Unit =
    results.foreach(write)

  /** Flush any buffered results to the underlying storage. */
  def flush(): Unit

  /** Close the sink and release resources. */
  def close(): Unit
}

object DataQualitySink {

  /**
   * Creates a file-based sink that writes results as JSON lines.
   *
   * @param path Path to the output file
   * @return A new file sink
   */
  def file(path: String): DataQualitySink = new FileDataQualitySink(path)

  /**
   * Creates a no-op sink that discards all results.
   * Useful for testing or when results are handled elsewhere.
   */
  val noop: DataQualitySink = new DataQualitySink {
    override def write(result: DataQualityResult): Unit = ()
    override def flush(): Unit                          = ()
    override def close(): Unit                          = ()
  }

  /**
   * Creates a sink that logs results using Log4j2.
   *
   * @param logLevel Log level to use (info, warn, debug)
   * @return A logging sink
   */
  def logging(logLevel: String = "info"): DataQualitySink =
    new LoggingDataQualitySink(logLevel)

  /**
   * Composes multiple sinks into one.
   * Results are written to all sinks in order.
   *
   * @param sinks The sinks to compose
   * @return A composite sink
   */
  def compose(sinks: DataQualitySink*): DataQualitySink = new CompositeDataQualitySink(sinks)
}

/**
 * File-based sink that writes results as JSON lines.
 *
 * Each result is written as a single JSON line, making the output
 * easy to process with standard tools like jq or log aggregators.
 *
 * @param path Path to the output file
 */
class FileDataQualitySink(val path: String) extends DataQualitySink {

  private lazy val writer: PrintWriter = new PrintWriter(new BufferedWriter(new FileWriter(path, true)))

  override def write(result: DataQualityResult): Unit = {
    val json = DataQualityResultSerializer.toJson(result)
    writer.println(json)
  }

  override def flush(): Unit = writer.flush()

  override def close(): Unit = writer.close()
}

/**
 * Logging sink that writes results to Log4j2.
 *
 * @param logLevel Log level to use (info, warn, debug)
 */
class LoggingDataQualitySink(logLevel: String) extends DataQualitySink {

  private val logger: Logger = LogManager.getLogger(classOf[LoggingDataQualitySink])

  override def write(result: DataQualityResult): Unit = {
    val message = formatResult(result)
    logLevel.toLowerCase match {
      case "debug" => logger.debug(message)
      case "warn"  => logger.warn(message)
      case _       => logger.info(message)
    }
  }

  private def formatResult(result: DataQualityResult): String = result match {
    case DataQualityResult.Passed(name, table, details, _) =>
      s"DQ PASSED: $name on $table${formatDetails(details)}"
    case DataQualityResult.Failed(name, table, message, details, _) =>
      s"DQ FAILED: $name on $table - $message${formatDetails(details)}"
    case DataQualityResult.Warning(name, table, message, details, _) =>
      s"DQ WARNING: $name on $table - $message${formatDetails(details)}"
    case DataQualityResult.Skipped(name, table, reason, _) =>
      s"DQ SKIPPED: $name on $table - $reason"
  }

  private def formatDetails(details: Map[String, Any]): String =
    if (details.isEmpty) ""
    else s" [${details.map { case (k, v) => s"$k=$v" }.mkString(", ")}]"

  override def flush(): Unit = ()

  override def close(): Unit = ()
}

/**
 * Composite sink that writes to multiple sinks.
 *
 * @param sinks The underlying sinks
 */
class CompositeDataQualitySink(sinks: Seq[DataQualitySink]) extends DataQualitySink {

  override def write(result: DataQualityResult): Unit =
    sinks.foreach(_.write(result))

  override def flush(): Unit =
    sinks.foreach(_.flush())

  override def close(): Unit =
    sinks.foreach(_.close())
}

/** Serializer for DataQualityResult to JSON. */
object DataQualityResultSerializer {

  private val timestampFormatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

  /**
   * Converts a DataQualityResult to JSON string.
   *
   * @param result The result to serialize
   * @return JSON string representation
   */
  def toJson(result: DataQualityResult): String = result match {
    case DataQualityResult.Passed(checkName, tableName, details, timestamp) =>
      s"""{"status":"passed","check_name":"$checkName","table_name":"$tableName","timestamp":"${timestampFormatter
          .format(timestamp)}","details":${mapToJson(details)}}"""

    case DataQualityResult.Failed(checkName, tableName, message, details, timestamp) =>
      s"""{"status":"failed","check_name":"$checkName","table_name":"$tableName","message":"${escapeJson(
          message)}","timestamp":"${timestampFormatter.format(timestamp)}","details":${mapToJson(details)}}"""

    case DataQualityResult.Warning(checkName, tableName, message, details, timestamp) =>
      s"""{"status":"warning","check_name":"$checkName","table_name":"$tableName","message":"${escapeJson(
          message)}","timestamp":"${timestampFormatter.format(timestamp)}","details":${mapToJson(details)}}"""

    case DataQualityResult.Skipped(checkName, tableName, reason, timestamp) =>
      s"""{"status":"skipped","check_name":"$checkName","table_name":"$tableName","reason":"${escapeJson(
          reason)}","timestamp":"${timestampFormatter.format(timestamp)}"}"""
  }

  private def mapToJson(map: Map[String, Any]): String =
    if (map.isEmpty) "{}"
    else {
      val entries = map.map { case (k, v) =>
        val valueJson = v match {
          case s: String  => s""""${escapeJson(s)}""""
          case n: Number  => n.toString
          case b: Boolean => b.toString
          case null       => "null"
          case other      => s""""${escapeJson(other.toString)}""""
        }
        s""""$k":$valueJson"""
      }
      s"{${entries.mkString(",")}}"
    }

  private def escapeJson(s: String): String =
    s.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
}
