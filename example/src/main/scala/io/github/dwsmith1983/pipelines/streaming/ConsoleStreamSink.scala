package io.github.dwsmith1983.pipelines.streaming

import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSink
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import pureconfig._
import pureconfig.generic.auto._

/**
 * A streaming sink that writes to the console.
 *
 * This sink is primarily useful for debugging and demonstration purposes.
 * It prints each batch to stdout, which is helpful during development.
 *
 * == Configuration ==
 *
 * {{{
 * instance-config {
 *   checkpoint-location = "/tmp/checkpoints/console-sink"
 *   output-mode = "append"
 *   num-rows = 20
 *   truncate = true
 * }
 * }}}
 *
 * @param conf The configuration for this sink
 */
class ConsoleStreamSink(conf: ConsoleStreamSink.Config) extends StreamingSink {

  override def writeStream(df: DataFrame): DataStreamWriter[Row] = {
    logger.info(s"Creating console sink: numRows=${conf.numRows}, truncate=${conf.truncate}")

    df.writeStream
      .format("console")
      .option("numRows", conf.numRows)
      .option("truncate", conf.truncate)
  }

  override def outputMode: OutputMode = conf.outputMode.toLowerCase match {
    case "append"   => OutputMode.Append()
    case "complete" => OutputMode.Complete()
    case "update"   => OutputMode.Update()
    case other =>
      throw new IllegalArgumentException(
        s"Invalid output mode: '$other'. Must be one of: append, complete, update"
      )
  }

  override def checkpointLocation: String = conf.checkpointLocation

  override def queryName: Option[String] = conf.queryName
}

/** Factory for creating ConsoleStreamSink instances from configuration. */
object ConsoleStreamSink extends ConfigurableInstance {

  /**
   * Configuration for ConsoleStreamSink.
   *
   * @param checkpointLocation Path for checkpoint storage
   * @param outputMode Output mode: append, complete, or update
   * @param numRows Number of rows to display per batch
   * @param truncate Whether to truncate long strings
   * @param queryName Optional name for the streaming query
   */
  case class Config(
    checkpointLocation: String,
    outputMode: String = "append",
    numRows: Int = 20,
    truncate: Boolean = true,
    queryName: Option[String] = None)

  override def createFromConfig(conf: com.typesafe.config.Config): ConsoleStreamSink =
    new ConsoleStreamSink(ConfigSource.fromConfig(conf).loadOrThrow[Config])
}
