package io.github.dwsmith1983.spark.pipeline.streaming

import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow
import org.apache.spark.sql.DataFrame

/**
 * Base trait for streaming data sources.
 *
 * Implement this trait to create custom streaming sources that can be
 * used with [[StreamingPipeline]]. The source provides a streaming
 * DataFrame via the [[readStream]] method.
 *
 * StreamingSource extends [[io.github.dwsmith1983.spark.pipeline.runtime.DataFlow DataFlow]]
 * to provide access to the SparkSession and logging facilities.
 *
 * == Important ==
 *
 * '''Do not call `run()` directly on a StreamingSource.''' The `run()`
 * method is intentionally disabled because streaming sources must be
 * composed with sinks through [[StreamingPipeline]]. Calling `run()`
 * will throw an `UnsupportedOperationException`.
 *
 * == Watermarking ==
 *
 * For sources that support event-time processing, you can optionally
 * specify watermark configuration via [[watermarkColumn]] and
 * [[watermarkDelay]]. The StreamingPipeline will apply the watermark
 * automatically if both are defined.
 *
 * == Example ==
 *
 * {{{
 * import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
 * import pureconfig._
 * import pureconfig.generic.auto._
 *
 * object KafkaSource extends ConfigurableInstance {
 *   case class Config(
 *     bootstrapServers: String,
 *     topic: String,
 *     startingOffsets: String = "latest"
 *   )
 *
 *   override def createFromConfig(conf: com.typesafe.config.Config): KafkaSource =
 *     new KafkaSource(ConfigSource.fromConfig(conf).loadOrThrow[Config])
 * }
 *
 * class KafkaSource(conf: KafkaSource.Config) extends StreamingSource {
 *   override def readStream(): DataFrame = {
 *     spark.readStream
 *       .format("kafka")
 *       .option("kafka.bootstrap.servers", conf.bootstrapServers)
 *       .option("subscribe", conf.topic)
 *       .option("startingOffsets", conf.startingOffsets)
 *       .load()
 *   }
 *
 *   override def watermarkColumn: Option[String] = Some("timestamp")
 *   override def watermarkDelay: Option[String] = Some("10 seconds")
 * }
 * }}}
 *
 * @see [[StreamingSink]] for writing streaming data
 * @see [[StreamingPipeline]] for orchestrating streaming pipelines
 */
trait StreamingSource extends DataFlow {

  /**
   * Returns a streaming DataFrame to be consumed by a StreamingPipeline.
   *
   * The returned DataFrame must have `isStreaming == true`. This is
   * typically achieved by using `spark.readStream` rather than
   * `spark.read`.
   *
   * @return A streaming DataFrame
   */
  def readStream(): DataFrame

  /**
   * Optional column name containing event timestamps for watermarking.
   *
   * If both `watermarkColumn` and `watermarkDelay` are defined, the
   * StreamingPipeline will automatically apply watermarking using:
   * {{{
   * df.withWatermark(watermarkColumn, watermarkDelay)
   * }}}
   *
   * @return The column name for event time, or None to disable watermarking
   */
  def watermarkColumn: Option[String] = None

  /**
   * Optional watermark delay threshold.
   *
   * This specifies how late data can arrive before it's dropped.
   * Common values include "10 seconds", "1 minute", "1 hour".
   *
   * @return The delay threshold as a string, or None to disable watermarking
   */
  def watermarkDelay: Option[String] = None

  /**
   * StreamingSource does not support direct execution via run().
   *
   * Streaming sources must be composed with sinks through StreamingPipeline.
   * This method always throws an `UnsupportedOperationException`.
   */
  override final def run(): Unit =
    throw new UnsupportedOperationException(
      s"StreamingSource.run() should not be called directly. " +
        s"Use StreamingPipeline to compose source '$name' with a sink."
    )
}
