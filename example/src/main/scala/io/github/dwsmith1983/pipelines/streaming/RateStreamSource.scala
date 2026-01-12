package io.github.dwsmith1983.pipelines.streaming

import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSource
import org.apache.spark.sql.DataFrame
import pureconfig._
import pureconfig.generic.auto._

/**
 * A streaming source that generates rows at a configurable rate.
 *
 * This source is primarily useful for testing and demonstration purposes.
 * It uses Spark's built-in "rate" source which generates rows with
 * incrementing values and timestamps.
 *
 * == Generated Schema ==
 *
 * {{{
 * root
 *  |-- timestamp: timestamp (nullable = true)
 *  |-- value: long (nullable = true)
 * }}}
 *
 * == Configuration ==
 *
 * {{{
 * instance-config {
 *   rows-per-second = 10
 *   ramp-up-time-seconds = 0
 *   num-partitions = 1
 * }
 * }}}
 *
 * @param conf The configuration for this source
 */
class RateStreamSource(conf: RateStreamSource.Config) extends StreamingSource {

  override def readStream(): DataFrame = {
    logger.info(s"Creating rate source: ${conf.rowsPerSecond} rows/sec")

    spark.readStream
      .format("rate")
      .option("rowsPerSecond", conf.rowsPerSecond)
      .option("rampUpTime", s"${conf.rampUpTimeSeconds}s")
      .option("numPartitions", conf.numPartitions)
      .load()
  }

  override def watermarkColumn: Option[String] =
    if (conf.enableWatermark) Some("timestamp") else None

  override def watermarkDelay: Option[String] =
    if (conf.enableWatermark) Some(conf.watermarkDelay) else None
}

/**
 * Factory for creating RateStreamSource instances from configuration.
 */
object RateStreamSource extends ConfigurableInstance {

  /**
   * Configuration for RateStreamSource.
   *
   * @param rowsPerSecond Number of rows to generate per second
   * @param rampUpTimeSeconds Time to ramp up to full rate
   * @param numPartitions Number of partitions for the generated data
   * @param enableWatermark Whether to enable watermarking on timestamp
   * @param watermarkDelay Watermark delay if enabled
   */
  case class Config(
    rowsPerSecond: Int = 10,
    rampUpTimeSeconds: Int = 0,
    numPartitions: Int = 1,
    enableWatermark: Boolean = false,
    watermarkDelay: String = "10 seconds"
  )

  override def createFromConfig(conf: com.typesafe.config.Config): RateStreamSource =
    new RateStreamSource(ConfigSource.fromConfig(conf).loadOrThrow[Config])
}
