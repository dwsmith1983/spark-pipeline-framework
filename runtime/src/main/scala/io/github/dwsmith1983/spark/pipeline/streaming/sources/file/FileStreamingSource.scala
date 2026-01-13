package io.github.dwsmith1983.spark.pipeline.streaming.sources.file

import com.typesafe.config.Config
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSource
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructType}
import pureconfig._
import pureconfig.generic.auto._

/**
 * Streaming source for files (JSON, CSV, Parquet, Avro, ORC).
 *
 * Monitors a directory for new files and processes them as a stream.
 * Supports various file formats with format-specific options.
 *
 * == Basic Usage ==
 *
 * {{{
 * val config = FileStreamConfig(
 *   path = "/data/incoming",
 *   format = FileFormat.Json,
 *   schemaPath = Some("/schemas/events.json")
 * )
 * val source = new FileStreamingSource(config)
 * val df = source.readStream()
 * }}}
 *
 * == Schema Requirement ==
 *
 * For file streaming, a schema is typically required. You can provide it via:
 * - `schemaPath`: Path to a JSON file containing the schema
 * - `schemaJson`: Inline JSON schema string
 *
 * {{{
 * // Using schema file
 * val config = FileStreamConfig(
 *   path = "/data/incoming",
 *   format = FileFormat.Json,
 *   schemaPath = Some("/schemas/events.json")
 * )
 *
 * // Using inline schema
 * val config = FileStreamConfig(
 *   path = "/data/incoming",
 *   format = FileFormat.Csv,
 *   schemaJson = Some("""{"fields":[{"name":"id","type":"integer"}]}""")
 * )
 * }}}
 *
 * @param config File streaming configuration
 *
 * @see [[FileStreamConfig]] for configuration options
 */
class FileStreamingSource(config: FileStreamConfig) extends StreamingSource {

  override def name: String =
    s"FileStreamingSource[${config.format.sparkFormat}:${config.path}]"

  /**
   * Creates a streaming DataFrame from the file source.
   *
   * @return Streaming DataFrame with file contents
   */
  override def readStream(): DataFrame = {
    val reader = spark.readStream.format(config.format.sparkFormat)

    // Apply schema if provided
    resolveSchema().foreach(reader.schema)

    // Apply common options
    reader
      .option("maxFilesPerTrigger", config.maxFilesPerTrigger)
      .option("latestFirst", config.latestFirst)

    // Apply format-specific options
    config.formatOptions.foreach {
      case (key, value) =>
        reader.option(key, value)
    }

    reader.load(config.path)
  }

  /**
   * Resolves the schema from configuration.
   *
   * @return Optional StructType schema
   */
  private def resolveSchema(): Option[StructType] =
    config.schemaPath
      .map { path =>
        val schemaJson = spark.read.text(path).collect().map(_.getString(0)).mkString
        DataType.fromJson(schemaJson).asInstanceOf[StructType]
      }
      .orElse {
        config.schemaJson.map(json => DataType.fromJson(json).asInstanceOf[StructType])
      }

  override def watermarkColumn: Option[String] = config.watermarkColumn
  override def watermarkDelay: Option[String]  = config.watermarkDelay
}

/** Supported file formats for streaming. */
sealed trait FileFormat {
  def sparkFormat: String
}

/** File format implementations. */
object FileFormat {
  case object Json    extends FileFormat { val sparkFormat = "json"    }
  case object Csv     extends FileFormat { val sparkFormat = "csv"     }
  case object Parquet extends FileFormat { val sparkFormat = "parquet" }
  case object Avro    extends FileFormat { val sparkFormat = "avro"    }
  case object Orc     extends FileFormat { val sparkFormat = "orc"     }
  case object Text    extends FileFormat { val sparkFormat = "text"    }

  def fromString(s: String): FileFormat = s.toLowerCase match {
    case "json"    => Json
    case "csv"     => Csv
    case "parquet" => Parquet
    case "avro"    => Avro
    case "orc"     => Orc
    case "text"    => Text
    case other     => throw new IllegalArgumentException(s"Unknown file format: $other")
  }
}

/**
 * Configuration for file streaming source.
 *
 * @param path               Directory path to monitor for new files
 * @param format             File format (json, csv, parquet, avro, orc, text)
 * @param schemaPath         Path to JSON schema file
 * @param schemaJson         Inline JSON schema string
 * @param maxFilesPerTrigger Maximum files to process per micro-batch
 * @param latestFirst        Process newest files first
 * @param formatOptions      Format-specific options (e.g., CSV delimiter)
 * @param watermarkColumn    Column for watermark (event time)
 * @param watermarkDelay     Watermark delay threshold
 */
final case class FileStreamConfig(
  path: String,
  format: FileFormat = FileFormat.Json,
  schemaPath: Option[String] = None,
  schemaJson: Option[String] = None,
  maxFilesPerTrigger: Int = 1000,
  latestFirst: Boolean = false,
  formatOptions: Map[String, String] = Map.empty,
  watermarkColumn: Option[String] = None,
  watermarkDelay: Option[String] = None) {

  def validate(): Unit =
    require(path.nonEmpty, "path cannot be empty")
}

/**
 * Factory for creating [[FileStreamingSource]] from configuration.
 *
 * == HOCON Configuration Example ==
 *
 * {{{
 * source {
 *   instance-class = "io.github.dwsmith1983.spark.pipeline.streaming.sources.file.FileStreamingSource"
 *   instance-config {
 *     path = "/data/incoming"
 *     format = "json"
 *     schema-path = "/schemas/events.json"
 *     max-files-per-trigger = 100
 *     latest-first = true
 *     format-options {
 *       multiLine = "true"
 *     }
 *   }
 * }
 * }}}
 */
object FileStreamingSource extends ConfigurableInstance {

  private case class HoconConfig(
    path: String,
    format: String = "json",
    schemaPath: Option[String] = None,
    schemaJson: Option[String] = None,
    maxFilesPerTrigger: Int = 1000,
    latestFirst: Boolean = false,
    formatOptions: Map[String, String] = Map.empty,
    watermarkColumn: Option[String] = None,
    watermarkDelay: Option[String] = None)

  override def createFromConfig(conf: Config): FileStreamingSource = {
    val hoconConfig = ConfigSource.fromConfig(conf).loadOrThrow[HoconConfig]

    val config = FileStreamConfig(
      path = hoconConfig.path,
      format = FileFormat.fromString(hoconConfig.format),
      schemaPath = hoconConfig.schemaPath,
      schemaJson = hoconConfig.schemaJson,
      maxFilesPerTrigger = hoconConfig.maxFilesPerTrigger,
      latestFirst = hoconConfig.latestFirst,
      formatOptions = hoconConfig.formatOptions,
      watermarkColumn = hoconConfig.watermarkColumn,
      watermarkDelay = hoconConfig.watermarkDelay
    )

    config.validate()
    new FileStreamingSource(config)
  }
}
