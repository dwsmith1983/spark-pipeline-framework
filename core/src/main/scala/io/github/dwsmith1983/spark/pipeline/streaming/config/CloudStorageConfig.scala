package io.github.dwsmith1983.spark.pipeline.streaming.config

/**
 * File format for cloud storage output.
 *
 * Supported formats for streaming writes to cloud storage (S3, GCS, ADLS).
 */
sealed trait FileFormat {
  def sparkFormat: String
}

object FileFormat {
  case object Parquet extends FileFormat { val sparkFormat = "parquet" }
  case object Json extends FileFormat { val sparkFormat = "json" }
  case object Csv extends FileFormat { val sparkFormat = "csv" }
  case object Avro extends FileFormat { val sparkFormat = "avro" }
  case object Orc extends FileFormat { val sparkFormat = "orc" }

  /**
   * Parse a file format from string.
   *
   * @param s Format string (case-insensitive): parquet, json, csv, avro, orc
   * @return The corresponding FileFormat
   */
  def fromString(s: String): FileFormat = s.toLowerCase match {
    case "parquet" => Parquet
    case "json"    => Json
    case "csv"     => Csv
    case "avro"    => Avro
    case "orc"     => Orc
    case other =>
      throw new IllegalArgumentException(
        s"Unknown file format: '$other'. Supported: parquet, json, csv, avro, orc"
      )
  }
}

/**
 * Configuration for cloud storage streaming sink.
 *
 * This sink writes streaming data to cloud object storage (S3, GCS, ADLS)
 * in various file formats. The sink automatically handles partitioning
 * and file naming.
 *
 * == Supported Cloud Providers ==
 *
 * - '''Amazon S3''': Use `s3://bucket/path` or `s3a://bucket/path`
 * - '''Google Cloud Storage''': Use `gs://bucket/path`
 * - '''Azure Data Lake Storage''': Use `abfss://container@account.dfs.core.windows.net/path`
 *
 * == File Formats ==
 *
 * - '''Parquet''' (default): Columnar, compressed, best for analytics
 * - '''JSON''': Human-readable, schema-flexible
 * - '''CSV''': Simple, widely compatible
 * - '''Avro''': Schema evolution support
 * - '''ORC''': Optimized row columnar
 *
 * == Example HOCON ==
 *
 * {{{
 * instance-config {
 *   path = "s3://my-bucket/streaming/events"
 *   checkpoint-path = "/checkpoints/s3-sink"
 *   format = "parquet"
 *   partition-by = ["year", "month", "day"]
 *   options {
 *     compression = "snappy"
 *   }
 * }
 * }}}
 *
 * @param path Cloud storage path (s3://, gs://, abfss://)
 * @param checkpointPath Path for Spark checkpoint storage
 * @param format Output file format (default: parquet)
 * @param partitionBy Columns to partition output by
 * @param queryName Optional name for the streaming query
 * @param options Additional format-specific options
 */
case class CloudStorageConfig(
    path: String,
    checkpointPath: String,
    format: String = "parquet",
    partitionBy: List[String] = List.empty,
    queryName: Option[String] = None,
    options: Map[String, String] = Map.empty) {

  /**
   * Get the parsed file format.
   */
  def fileFormat: FileFormat = FileFormat.fromString(format)
}
