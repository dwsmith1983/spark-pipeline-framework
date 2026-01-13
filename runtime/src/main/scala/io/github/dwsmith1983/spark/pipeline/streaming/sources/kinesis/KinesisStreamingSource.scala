package io.github.dwsmith1983.spark.pipeline.streaming.sources.kinesis

import com.typesafe.config.Config
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSource
import org.apache.spark.sql.DataFrame
import pureconfig._
import pureconfig.generic.auto._

/**
 * Streaming source for Amazon Kinesis Data Streams.
 *
 * Reads data from Kinesis streams using the Spark Kinesis connector.
 * Supports IAM authentication, cross-account access, and various
 * starting position configurations.
 *
 * == Basic Usage ==
 *
 * {{{
 * val config = KinesisSourceConfig(
 *   streamName = "my-stream",
 *   region = "us-east-1"
 * )
 * val source = new KinesisStreamingSource(config)
 * val df = source.readStream()
 * }}}
 *
 * == With IAM Role Assumption ==
 *
 * For cross-account access:
 *
 * {{{
 * val config = KinesisSourceConfig(
 *   streamName = "my-stream",
 *   region = "us-east-1",
 *   roleArn = Some("arn:aws:iam::123456789012:role/KinesisReadRole"),
 *   roleSessionName = Some("spark-streaming")
 * )
 * }}}
 *
 * == Output Schema ==
 *
 * The output DataFrame includes:
 * {{{
 * root
 *  |-- data: binary (the record payload)
 *  |-- partitionKey: string
 *  |-- sequenceNumber: string
 *  |-- approximateArrivalTimestamp: timestamp
 *  |-- stream: string
 *  |-- shardId: string
 * }}}
 *
 * == Dependencies ==
 *
 * Requires the Spark Kinesis connector:
 * {{{
 * "software.amazon.kinesis" % "amazon-kinesis-connector-spark" % "x.y.z"
 * }}}
 *
 * @param config Kinesis source configuration
 *
 * @see [[KinesisSourceConfig]] for configuration options
 */
class KinesisStreamingSource(config: KinesisSourceConfig) extends StreamingSource {

  config.validate()

  override def name: String =
    s"KinesisStreamingSource[${config.streamName}@${config.region}]"

  /**
   * Creates a streaming DataFrame from the Kinesis stream.
   *
   * @return Streaming DataFrame with Kinesis record schema
   */
  override def readStream(): DataFrame = {
    val reader = spark.readStream.format("kinesis")

    // Apply required options
    reader
      .option("streamName", config.streamName)
      .option("region", config.region)
      .option("startingPosition", config.startingPosition.toKinesisString)

    // Apply endpoint override (for LocalStack or custom endpoints)
    config.endpointUrl.foreach(reader.option("endpointUrl", _))

    // Apply IAM role assumption
    config.roleArn.foreach(reader.option("roleArn", _))
    config.roleSessionName.foreach(reader.option("roleSessionName", _))

    // Apply performance options
    config.maxFetchRecordsPerShard.foreach(n => reader.option("maxFetchRecordsPerShard", n))
    config.maxFetchTimePerShardSec.foreach(n => reader.option("maxFetchTimePerShardSec", n))

    // Apply additional options
    config.options.foreach {
      case (key, value) =>
        reader.option(key, value)
    }

    reader.load()
  }

  override def watermarkColumn: Option[String] = config.watermarkColumn
  override def watermarkDelay: Option[String]  = config.watermarkDelay
}

/** Starting position for Kinesis streaming. */
sealed trait KinesisStartingPosition {
  def toKinesisString: String
}

object KinesisStartingPosition {

  case object TrimHorizon extends KinesisStartingPosition {
    override def toKinesisString: String = "TRIM_HORIZON"
  }

  case object Latest extends KinesisStartingPosition {
    override def toKinesisString: String = "LATEST"
  }

  final case class AtTimestamp(epochMs: Long) extends KinesisStartingPosition {
    override def toKinesisString: String = s"AT_TIMESTAMP/$epochMs"
  }

  def fromString(s: String): KinesisStartingPosition = s.toUpperCase match {
    case "TRIM_HORIZON" | "EARLIEST" => TrimHorizon
    case "LATEST"                    => Latest
    case ts if ts.startsWith("AT_TIMESTAMP/") =>
      AtTimestamp(ts.stripPrefix("AT_TIMESTAMP/").toLong)
    case other =>
      throw new IllegalArgumentException(s"Invalid Kinesis starting position: $other")
  }
}

/**
 * Configuration for Kinesis streaming source.
 *
 * @param streamName                Stream name
 * @param region                    AWS region
 * @param endpointUrl               Custom endpoint URL (for LocalStack)
 * @param startingPosition          Where to start reading
 * @param roleArn                   IAM role ARN for cross-account access
 * @param roleSessionName           Session name when assuming role
 * @param maxFetchRecordsPerShard   Max records per shard per fetch
 * @param maxFetchTimePerShardSec   Max fetch time per shard in seconds
 * @param watermarkColumn           Column for watermark
 * @param watermarkDelay            Watermark delay threshold
 * @param options                   Additional connector options
 */
final case class KinesisSourceConfig(
  streamName: String,
  region: String,
  endpointUrl: Option[String] = None,
  startingPosition: KinesisStartingPosition = KinesisStartingPosition.Latest,
  roleArn: Option[String] = None,
  roleSessionName: Option[String] = None,
  maxFetchRecordsPerShard: Option[Int] = None,
  maxFetchTimePerShardSec: Option[Int] = None,
  watermarkColumn: Option[String] = None,
  watermarkDelay: Option[String] = None,
  options: Map[String, String] = Map.empty) {

  def validate(): Unit = {
    require(streamName.nonEmpty, "streamName cannot be empty")
    require(region.nonEmpty, "region cannot be empty")
  }
}

/**
 * Factory for creating [[KinesisStreamingSource]] from configuration.
 *
 * == HOCON Configuration Example ==
 *
 * {{{
 * source {
 *   instance-class = "io.github.dwsmith1983.spark.pipeline.streaming.sources.kinesis.KinesisStreamingSource"
 *   instance-config {
 *     stream-name = "my-stream"
 *     region = "us-east-1"
 *     starting-position = "LATEST"
 *     role-arn = "arn:aws:iam::123456789012:role/KinesisReadRole"
 *     watermark-column = "approximateArrivalTimestamp"
 *     watermark-delay = "30 seconds"
 *   }
 * }
 * }}}
 */
object KinesisStreamingSource extends ConfigurableInstance {

  private case class HoconConfig(
    streamName: String,
    region: String,
    endpointUrl: Option[String] = None,
    startingPosition: String = "LATEST",
    roleArn: Option[String] = None,
    roleSessionName: Option[String] = None,
    maxFetchRecordsPerShard: Option[Int] = None,
    maxFetchTimePerShardSec: Option[Int] = None,
    watermarkColumn: Option[String] = None,
    watermarkDelay: Option[String] = None,
    options: Map[String, String] = Map.empty)

  override def createFromConfig(conf: Config): KinesisStreamingSource = {
    val hoconConfig = ConfigSource.fromConfig(conf).loadOrThrow[HoconConfig]

    val config = KinesisSourceConfig(
      streamName = hoconConfig.streamName,
      region = hoconConfig.region,
      endpointUrl = hoconConfig.endpointUrl,
      startingPosition = KinesisStartingPosition.fromString(hoconConfig.startingPosition),
      roleArn = hoconConfig.roleArn,
      roleSessionName = hoconConfig.roleSessionName,
      maxFetchRecordsPerShard = hoconConfig.maxFetchRecordsPerShard,
      maxFetchTimePerShardSec = hoconConfig.maxFetchTimePerShardSec,
      watermarkColumn = hoconConfig.watermarkColumn,
      watermarkDelay = hoconConfig.watermarkDelay,
      options = hoconConfig.options
    )

    new KinesisStreamingSource(config)
  }
}
