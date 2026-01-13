package io.github.dwsmith1983.spark.pipeline.streaming.sinks

import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSink
import io.github.dwsmith1983.spark.pipeline.streaming.config.{DeltaLakeConfig, DeltaWriteMode}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import pureconfig._
import pureconfig.generic.auto._

/**
 * Streaming sink that writes to Delta Lake tables.
 *
 * This sink supports three write modes:
 *
 * - '''Append''': Simply append new rows to the table (default, most efficient)
 * - '''Merge''': Upsert rows based on a merge condition using foreachBatch
 * - '''Overwrite''': Replace data, optionally filtered by `replaceWhere`
 *
 * == Append Mode ==
 *
 * The simplest and most efficient mode. Data is streamed directly to
 * the Delta table using Spark's native Delta streaming writer.
 *
 * {{{
 * val config = DeltaLakeConfig.forAppend("/data/events")
 * val sink = new DeltaLakeStreamSink(config, checkpointPath)
 * }}}
 *
 * == Merge Mode ==
 *
 * Performs upserts using Delta Lake's MERGE INTO operation. This mode
 * uses `foreachBatch` to execute merge operations on each micro-batch.
 *
 * {{{
 * val config = DeltaLakeConfig.forMerge(
 *   path = "/data/events",
 *   mergeCondition = "target.id = source.id"
 * )
 * val sink = new DeltaLakeStreamSink(config, checkpointPath)
 * }}}
 *
 * == Overwrite Mode ==
 *
 * Replaces data in the table. Use `replaceWhere` to limit the scope
 * of the overwrite to specific partitions.
 *
 * {{{
 * val config = DeltaLakeConfig(
 *   path = "/data/events",
 *   writeMode = DeltaWriteMode.Overwrite,
 *   replaceWhere = Some("date >= '2024-01-01'")
 * )
 * }}}
 *
 * @param config The Delta Lake configuration
 * @param checkpointPath Path for Spark checkpoint storage
 * @param queryName Optional name for the streaming query
 */
class DeltaLakeStreamSink(
  config: DeltaLakeConfig,
  checkpointPath: String,
  override val queryName: Option[String] = None)
  extends StreamingSink {

  config.validateForSink()

  override def writeStream(df: DataFrame): DataStreamWriter[Row] = {
    logger.info(
      s"Creating Delta Lake sink: path=${config.path}, " +
        s"writeMode=${config.writeMode}, " +
        s"mergeSchema=${config.mergeSchema}"
    )

    config.writeMode match {
      case DeltaWriteMode.Append    => createAppendWriter(df)
      case DeltaWriteMode.Merge     => createMergeWriter(df)
      case DeltaWriteMode.Overwrite => createOverwriteWriter(df)
    }
  }

  private def createAppendWriter(df: DataFrame): DataStreamWriter[Row] = {
    val writer = df.writeStream
      .format("delta")
      .option("path", config.path)

    val writerWithSchema = if (config.mergeSchema) {
      writer.option("mergeSchema", "true")
    } else {
      writer
    }

    val writerWithOptions = config.options.foldLeft(writerWithSchema) {
      case (w, (k, v)) =>
        w.option(k, v)
    }

    if (config.partitionBy.nonEmpty) {
      writerWithOptions.partitionBy(config.partitionBy: _*)
    } else {
      writerWithOptions
    }
  }

  private def createMergeWriter(df: DataFrame): DataStreamWriter[Row] = {
    val mergeCondition = config.mergeCondition.getOrElse(
      throw new IllegalStateException("mergeCondition required for Merge mode")
    )

    df.writeStream.foreachBatch { (batchDf: DataFrame, batchId: Long) =>
      if (!batchDf.isEmpty) {
        logger.debug(s"Processing merge batch $batchId")

        val tempViewName = s"delta_merge_batch_${batchId}_${System.currentTimeMillis()}"
        batchDf.createOrReplaceTempView(tempViewName)

        val mergeQuery =
          s"""
             |MERGE INTO delta.`${config.path}` AS target
             |USING $tempViewName AS source
             |ON $mergeCondition
             |WHEN MATCHED THEN UPDATE SET *
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin

        spark.sql(mergeQuery)
        spark.catalog.dropTempView(tempViewName)
      }
      ()
    }
  }

  private def createOverwriteWriter(df: DataFrame): DataStreamWriter[Row] =
    df.writeStream.foreachBatch { (batchDf: DataFrame, batchId: Long) =>
      if (!batchDf.isEmpty) {
        logger.debug(s"Processing overwrite batch $batchId")

        val writer = batchDf.write
          .format("delta")
          .mode("overwrite")

        val writerWithOptions = config.replaceWhere match {
          case Some(predicate) => writer.option("replaceWhere", predicate)
          case None            => writer
        }

        val writerWithPartitions = if (config.partitionBy.nonEmpty) {
          writerWithOptions.partitionBy(config.partitionBy: _*)
        } else {
          writerWithOptions
        }

        writerWithPartitions.save(config.path)
      }
      ()
    }

  override def outputMode: OutputMode = config.writeMode match {
    case DeltaWriteMode.Append    => OutputMode.Append()
    case DeltaWriteMode.Merge     => OutputMode.Update()
    case DeltaWriteMode.Overwrite => OutputMode.Complete()
  }

  override def checkpointLocation: String = checkpointPath
}

/**
 * Factory for creating DeltaLakeStreamSink instances from HOCON configuration.
 *
 * The configuration must include:
 * - All DeltaLakeConfig fields
 * - `checkpointPath`: Path for checkpoint storage
 * - `queryName` (optional): Name for the streaming query
 */
object DeltaLakeStreamSink extends ConfigurableInstance {

  /** Extended configuration for DeltaLakeStreamSink including checkpoint path. */
  case class SinkConfig(
    path: String,
    checkpointPath: String,
    readChangeDataFeed: Boolean = false,
    startingVersion: Option[Long] = None,
    startingTimestamp: Option[String] = None,
    ignoreChanges: Boolean = false,
    ignoreDeletes: Boolean = false,
    maxFilesPerTrigger: Int = 1000,
    writeMode: String = "append",
    mergeCondition: Option[String] = None,
    mergeSchema: Boolean = false,
    partitionBy: List[String] = List.empty,
    replaceWhere: Option[String] = None,
    schemaEvolutionMode: String = "addColumns",
    options: Map[String, String] = Map.empty,
    queryName: Option[String] = None)

  override def createFromConfig(conf: com.typesafe.config.Config): DeltaLakeStreamSink = {
    val sinkConfig = ConfigSource.fromConfig(conf).loadOrThrow[SinkConfig]

    val deltaWriteMode = sinkConfig.writeMode.toLowerCase match {
      case "append"    => DeltaWriteMode.Append
      case "merge"     => DeltaWriteMode.Merge
      case "overwrite" => DeltaWriteMode.Overwrite
      case other =>
        throw new IllegalArgumentException(
          s"Unknown Delta write mode: '$other'. Supported: append, merge, overwrite"
        )
    }

    val deltaConfig = DeltaLakeConfig(
      path = sinkConfig.path,
      writeMode = deltaWriteMode,
      mergeCondition = sinkConfig.mergeCondition,
      mergeSchema = sinkConfig.mergeSchema,
      partitionBy = sinkConfig.partitionBy,
      replaceWhere = sinkConfig.replaceWhere,
      options = sinkConfig.options
    )

    new DeltaLakeStreamSink(deltaConfig, sinkConfig.checkpointPath, sinkConfig.queryName)
  }
}
