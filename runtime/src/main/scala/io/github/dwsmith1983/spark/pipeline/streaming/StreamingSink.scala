package io.github.dwsmith1983.spark.pipeline.streaming

import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}

/**
 * Base trait for streaming data sinks.
 *
 * Implement this trait to create custom streaming sinks that can be
 * used with [[StreamingPipeline]]. The sink configures how streaming
 * data is written via the [[writeStream]] method.
 *
 * StreamingSink extends [[io.github.dwsmith1983.spark.pipeline.runtime.DataFlow DataFlow]]
 * to provide access to the SparkSession and logging facilities.
 *
 * == Important ==
 *
 * '''Do not call `run()` directly on a StreamingSink.''' The `run()`
 * method is intentionally disabled because streaming sinks must be
 * composed with sources through [[StreamingPipeline]]. Calling `run()`
 * will throw an `UnsupportedOperationException`.
 *
 * == Output Modes ==
 *
 * Spark Structured Streaming supports three output modes:
 *
 * - '''Append''': Only new rows are written. Use for most streaming workloads.
 * - '''Complete''': Entire result table is written. Use with aggregations.
 * - '''Update''': Only changed rows are written. Use with stateful operations.
 *
 * == Checkpointing ==
 *
 * Every streaming sink must specify a [[checkpointLocation]] for fault
 * tolerance. This location stores query progress and enables exactly-once
 * processing semantics.
 *
 * == Example ==
 *
 * {{{
 * import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
 * import org.apache.spark.sql.streaming.OutputMode
 * import pureconfig._
 * import pureconfig.generic.auto._
 *
 * object DeltaSink extends ConfigurableInstance {
 *   case class Config(
 *     tablePath: String,
 *     checkpointPath: String,
 *     outputMode: String = "append"
 *   )
 *
 *   override def createFromConfig(conf: com.typesafe.config.Config): DeltaSink =
 *     new DeltaSink(ConfigSource.fromConfig(conf).loadOrThrow[Config])
 * }
 *
 * class DeltaSink(conf: DeltaSink.Config) extends StreamingSink {
 *   override def writeStream(df: DataFrame): DataStreamWriter[Row] =
 *     df.writeStream
 *       .format("delta")
 *       .option("path", conf.tablePath)
 *
 *   override def outputMode: OutputMode = conf.outputMode match {
 *     case "append" => OutputMode.Append()
 *     case "complete" => OutputMode.Complete()
 *     case "update" => OutputMode.Update()
 *   }
 *
 *   override def checkpointLocation: String = conf.checkpointPath
 *
 *   override def queryName: Option[String] = Some("delta-sink")
 * }
 * }}}
 *
 * @see [[StreamingSource]] for reading streaming data
 * @see [[StreamingPipeline]] for orchestrating streaming pipelines
 */
trait StreamingSink extends DataFlow {

  /**
   * Returns a configured DataStreamWriter for the given DataFrame.
   *
   * The StreamingPipeline will apply output mode, trigger, checkpoint
   * location, and query name before calling `start()`. This method
   * should configure format-specific options like path, partitioning,
   * or connector settings.
   *
   * @param df The streaming DataFrame to write
   * @return A configured DataStreamWriter (do not call start())
   */
  def writeStream(df: DataFrame): DataStreamWriter[Row]

  /**
   * The output mode for this sink.
   *
   * - `OutputMode.Append()` - Only new rows (default for most sinks)
   * - `OutputMode.Complete()` - Entire result table (aggregations)
   * - `OutputMode.Update()` - Only changed rows (stateful operations)
   *
   * @return The Spark OutputMode for this sink
   */
  def outputMode: OutputMode

  /**
   * The checkpoint location for this streaming query.
   *
   * This path must be accessible to the Spark cluster and should be
   * durable storage (HDFS, S3, GCS, etc.) for production workloads.
   *
   * Each streaming query must have a unique checkpoint location. If you
   * have multiple sinks, use different paths for each.
   *
   * @return Absolute path to the checkpoint directory
   */
  def checkpointLocation: String

  /**
   * Optional name for the streaming query.
   *
   * Query names are useful for:
   * - Identifying queries in the Spark UI
   * - Monitoring and alerting
   * - Metrics collection via hooks
   *
   * @return The query name, or None to use auto-generated name
   */
  def queryName: Option[String] = None

  /**
   * StreamingSink does not support direct execution via run().
   *
   * Streaming sinks must be composed with sources through StreamingPipeline.
   * This method always throws an `UnsupportedOperationException`.
   */
  override final def run(): Unit =
    throw new UnsupportedOperationException(
      s"StreamingSink.run() should not be called directly. " +
        s"Use StreamingPipeline to compose a source with sink '${name}'."
    )
}
