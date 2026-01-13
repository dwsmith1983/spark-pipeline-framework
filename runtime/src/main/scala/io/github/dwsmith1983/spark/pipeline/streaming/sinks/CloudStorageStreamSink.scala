package io.github.dwsmith1983.spark.pipeline.streaming.sinks

import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSink
import io.github.dwsmith1983.spark.pipeline.streaming.config.CloudStorageConfig
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import pureconfig._
import pureconfig.generic.auto._

/**
 * Streaming sink that writes to cloud object storage.
 *
 * This sink supports writing streaming data to S3, GCS, or Azure Data Lake
 * Storage in various file formats. Files are automatically organized by
 * partition columns if specified.
 *
 * == Supported Cloud Providers ==
 *
 * - '''Amazon S3''': `s3://bucket/path` or `s3a://bucket/path`
 * - '''Google Cloud Storage''': `gs://bucket/path`
 * - '''Azure Data Lake Storage Gen2''': `abfss://container@account.dfs.core.windows.net/path`
 *
 * == File Formats ==
 *
 * - '''Parquet''' (default): Best for analytics workloads
 * - '''JSON''': Human-readable, schema-flexible
 * - '''CSV''': Simple, widely compatible
 * - '''Avro''': Good for schema evolution
 * - '''ORC''': Optimized for Hive integration
 *
 * == Partitioning ==
 *
 * Use `partitionBy` to organize output into partition directories:
 *
 * {{{
 * val config = CloudStorageConfig(
 *   path = "s3://bucket/events",
 *   checkpointPath = "/checkpoints/s3",
 *   partitionBy = List("year", "month", "day")
 * )
 * // Output: s3://bucket/events/year=2024/month=01/day=15/part-*.parquet
 * }}}
 *
 * == Example ==
 *
 * {{{
 * val config = CloudStorageConfig(
 *   path = "gs://my-bucket/streaming/output",
 *   checkpointPath = "/checkpoints/gcs",
 *   format = "parquet",
 *   options = Map("compression" -> "snappy")
 * )
 * val sink = new CloudStorageStreamSink(config)
 * }}}
 *
 * @param config The cloud storage sink configuration
 */
class CloudStorageStreamSink(config: CloudStorageConfig) extends StreamingSink {

  override def writeStream(df: DataFrame): DataStreamWriter[Row] = {
    val format = config.fileFormat

    logger.info(
      s"Creating cloud storage sink: path=${config.path}, " +
        s"format=${format.sparkFormat}, " +
        s"partitionBy=${config.partitionBy.mkString(",")}"
    )

    val writer = df.writeStream
      .format(format.sparkFormat)
      .option("path", config.path)

    val writerWithOptions = config.options.foldLeft(writer) {
      case (w, (k, v)) =>
        w.option(k, v)
    }

    if (config.partitionBy.nonEmpty) {
      writerWithOptions.partitionBy(config.partitionBy: _*)
    } else {
      writerWithOptions
    }
  }

  override def outputMode: OutputMode = OutputMode.Append()

  override def checkpointLocation: String = config.checkpointPath

  override def queryName: Option[String] = config.queryName
}

/** Factory for creating CloudStorageStreamSink instances from HOCON configuration. */
object CloudStorageStreamSink extends ConfigurableInstance {

  override def createFromConfig(conf: com.typesafe.config.Config): CloudStorageStreamSink =
    new CloudStorageStreamSink(ConfigSource.fromConfig(conf).loadOrThrow[CloudStorageConfig])
}
