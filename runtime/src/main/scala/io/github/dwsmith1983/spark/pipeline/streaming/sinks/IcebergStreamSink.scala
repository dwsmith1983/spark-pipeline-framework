package io.github.dwsmith1983.spark.pipeline.streaming.sinks

import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSink
import io.github.dwsmith1983.spark.pipeline.streaming.config.{IcebergConfig, IcebergWriteMode}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import pureconfig._
import pureconfig.generic.auto._

/**
 * Streaming sink that writes to Apache Iceberg tables.
 *
 * This sink supports three write modes:
 *
 * - '''Append''': Simply append new rows to the table (default, most efficient)
 * - '''Upsert''': Merge rows based on key columns
 * - '''OverwriteDynamic''': Dynamically overwrite partitions
 *
 * == Append Mode ==
 *
 * The simplest and most efficient mode. Data is streamed directly to
 * the Iceberg table using Spark's native Iceberg streaming writer.
 *
 * {{{
 * val config = IcebergConfig.forAppend("catalog.db.events")
 * val sink = new IcebergStreamSink(config, checkpointPath)
 * }}}
 *
 * == Upsert Mode ==
 *
 * Performs upserts based on specified key columns. This mode is useful
 * for maintaining current state tables from event streams.
 *
 * {{{
 * val config = IcebergConfig.forUpsert(
 *   tablePath = "catalog.db.events",
 *   upsertKeys = List("event_id")
 * )
 * val sink = new IcebergStreamSink(config, checkpointPath)
 * }}}
 *
 * == OverwriteDynamic Mode ==
 *
 * Dynamically overwrites partitions based on the data being written.
 * Only partitions present in the current batch are affected.
 *
 * {{{
 * val config = IcebergConfig(
 *   tablePath = "catalog.db.events",
 *   writeMode = IcebergWriteMode.OverwriteDynamic
 * )
 * val sink = new IcebergStreamSink(config, checkpointPath)
 * }}}
 *
 * == Fanout Writes ==
 *
 * Enable `fanoutEnabled` for better write parallelism when writing
 * to many partitions simultaneously.
 *
 * @param config The Iceberg configuration
 * @param checkpointPath Path for Spark checkpoint storage
 * @param queryName Optional name for the streaming query
 */
class IcebergStreamSink(
    config: IcebergConfig,
    checkpointPath: String,
    override val queryName: Option[String] = None)
    extends StreamingSink {

  config.validateForSink()

  override def writeStream(df: DataFrame): DataStreamWriter[Row] = {
    logger.info(
      s"Creating Iceberg sink: table=${config.fullTablePath}, " +
        s"writeMode=${config.writeMode}, " +
        s"fanout=${config.fanoutEnabled}"
    )

    config.writeMode match {
      case IcebergWriteMode.Append          => createAppendWriter(df)
      case IcebergWriteMode.Upsert          => createUpsertWriter(df)
      case IcebergWriteMode.OverwriteDynamic => createOverwriteDynamicWriter(df)
    }
  }

  private def createAppendWriter(df: DataFrame): DataStreamWriter[Row] = {
    val writer = df.writeStream
      .format("iceberg")
      .option("path", config.fullTablePath)

    val writerWithFanout = if (config.fanoutEnabled) {
      writer.option("fanout-enabled", "true")
    } else {
      writer
    }

    config.options.foldLeft(writerWithFanout) { case (w, (k, v)) =>
      w.option(k, v)
    }
  }

  private def createUpsertWriter(df: DataFrame): DataStreamWriter[Row] = {
    if (config.upsertKeys.isEmpty) {
      throw new IllegalStateException("upsertKeys required for Upsert mode")
    }

    val writer = df.writeStream
      .format("iceberg")
      .option("path", config.fullTablePath)
      .option("upsert-enabled", "true")
      .option("upsert-keys", config.upsertKeys.mkString(","))

    val writerWithFanout = if (config.fanoutEnabled) {
      writer.option("fanout-enabled", "true")
    } else {
      writer
    }

    config.options.foldLeft(writerWithFanout) { case (w, (k, v)) =>
      w.option(k, v)
    }
  }

  private def createOverwriteDynamicWriter(df: DataFrame): DataStreamWriter[Row] = {
    val writer = df.writeStream
      .format("iceberg")
      .option("path", config.fullTablePath)
      .option("overwrite-mode", "dynamic")

    val writerWithFanout = if (config.fanoutEnabled) {
      writer.option("fanout-enabled", "true")
    } else {
      writer
    }

    config.options.foldLeft(writerWithFanout) { case (w, (k, v)) =>
      w.option(k, v)
    }
  }

  override def outputMode: OutputMode = OutputMode.Append()

  override def checkpointLocation: String = checkpointPath
}

/**
 * Factory for creating IcebergStreamSink instances from HOCON configuration.
 *
 * The configuration must include:
 * - All IcebergConfig fields
 * - `checkpointPath`: Path for checkpoint storage
 * - `queryName` (optional): Name for the streaming query
 */
object IcebergStreamSink extends ConfigurableInstance {

  /**
   * Extended configuration for IcebergStreamSink including checkpoint path.
   */
  case class SinkConfig(
      tablePath: String,
      checkpointPath: String,
      catalogName: String = "spark_catalog",
      streamFromTimestamp: Option[Long] = None,
      skipDeleteSnapshots: Boolean = false,
      skipOverwriteSnapshots: Boolean = false,
      writeMode: String = "append",
      upsertKeys: List[String] = List.empty,
      fanoutEnabled: Boolean = false,
      partitionBy: List[String] = List.empty,
      schemaEvolutionMode: String = "addColumns",
      options: Map[String, String] = Map.empty,
      queryName: Option[String] = None)

  override def createFromConfig(conf: com.typesafe.config.Config): IcebergStreamSink = {
    val sinkConfig = ConfigSource.fromConfig(conf).loadOrThrow[SinkConfig]

    val icebergWriteMode = sinkConfig.writeMode.toLowerCase match {
      case "append"           => IcebergWriteMode.Append
      case "upsert"           => IcebergWriteMode.Upsert
      case "overwritedynamic" => IcebergWriteMode.OverwriteDynamic
      case "overwrite-dynamic" => IcebergWriteMode.OverwriteDynamic
      case other =>
        throw new IllegalArgumentException(
          s"Unknown Iceberg write mode: '$other'. Supported: append, upsert, overwriteDynamic"
        )
    }

    val icebergConfig = IcebergConfig(
      tablePath = sinkConfig.tablePath,
      catalogName = sinkConfig.catalogName,
      writeMode = icebergWriteMode,
      upsertKeys = sinkConfig.upsertKeys,
      fanoutEnabled = sinkConfig.fanoutEnabled,
      partitionBy = sinkConfig.partitionBy,
      options = sinkConfig.options
    )

    new IcebergStreamSink(icebergConfig, sinkConfig.checkpointPath, sinkConfig.queryName)
  }
}
