package io.github.dwsmith1983.spark.pipeline.streaming.sinks

import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSink
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import pureconfig._
import pureconfig.generic.auto._

/**
 * Configuration for console streaming sink.
 *
 * @param checkpointLocation Path for Spark checkpoint storage
 * @param outputMode Output mode: append, complete, or update
 * @param numRows Number of rows to display per batch
 * @param truncate Whether to truncate long strings
 * @param queryName Optional name for the streaming query
 */
case class ConsoleSinkConfig(
  checkpointLocation: String,
  outputMode: String = "append",
  numRows: Int = 20,
  truncate: Boolean = true,
  queryName: Option[String] = None)

/**
 * Streaming sink that writes to the console.
 *
 * This sink prints each micro-batch to stdout, making it useful for
 * debugging and development. For production, use a persistent sink
 * like Kafka, Delta Lake, or cloud storage.
 *
 * == Output Modes ==
 *
 * - '''Append''' (default): Shows only new rows in each batch
 * - '''Complete''': Shows all rows (use with aggregations)
 * - '''Update''': Shows changed rows (use with stateful operations)
 *
 * == Example ==
 *
 * {{{
 * val config = ConsoleSinkConfig(
 *   checkpointLocation = "/tmp/checkpoints/debug",
 *   numRows = 50,
 *   truncate = false
 * )
 * val sink = new ConsoleStreamSink(config)
 * }}}
 *
 * == HOCON Configuration ==
 *
 * {{{
 * instance-config {
 *   checkpoint-location = "/tmp/checkpoints/console"
 *   output-mode = "append"
 *   num-rows = 20
 *   truncate = true
 * }
 * }}}
 *
 * @param config The console sink configuration
 */
class ConsoleStreamSink(config: ConsoleSinkConfig) extends StreamingSink {

  override def writeStream(df: DataFrame): DataStreamWriter[Row] = {
    logger.info(
      s"Creating console sink: numRows=${config.numRows}, truncate=${config.truncate}"
    )

    df.writeStream
      .format("console")
      .option("numRows", config.numRows)
      .option("truncate", config.truncate)
  }

  override def outputMode: OutputMode = config.outputMode.toLowerCase match {
    case "append"   => OutputMode.Append()
    case "complete" => OutputMode.Complete()
    case "update"   => OutputMode.Update()
    case other =>
      throw new IllegalArgumentException(
        s"Invalid output mode: '$other'. Must be one of: append, complete, update"
      )
  }

  override def checkpointLocation: String = config.checkpointLocation

  override def queryName: Option[String] = config.queryName
}

/** Factory for creating ConsoleStreamSink instances from HOCON configuration. */
object ConsoleStreamSink extends ConfigurableInstance {

  override def createFromConfig(conf: com.typesafe.config.Config): ConsoleStreamSink =
    new ConsoleStreamSink(ConfigSource.fromConfig(conf).loadOrThrow[ConsoleSinkConfig])
}
