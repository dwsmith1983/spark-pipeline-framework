package io.github.dwsmith1983.spark.pipeline.streaming.sources.kafka

import com.typesafe.config.Config
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSource
import io.github.dwsmith1983.spark.pipeline.streaming.common.StartingOffsets
import org.apache.spark.sql.DataFrame
import pureconfig._
import pureconfig.generic.auto._

/**
 * Streaming source for Apache Kafka.
 *
 * Reads data from Kafka topics using Spark Structured Streaming. Supports
 * authentication (SASL, SSL), Schema Registry integration, and various
 * starting offset configurations.
 *
 * == Basic Usage ==
 *
 * {{{
 * val config = KafkaSourceConfig(
 *   bootstrapServers = "localhost:9092",
 *   topics = List("events")
 * )
 * val source = new KafkaStreamingSource(config)
 * val df = source.readStream()  // Returns streaming DataFrame
 * }}}
 *
 * == Output Schema ==
 *
 * The output DataFrame has the following schema:
 * {{{
 * root
 *  |-- key: binary (nullable = true)
 *  |-- value: binary (nullable = true)
 *  |-- topic: string (nullable = true)
 *  |-- partition: integer (nullable = true)
 *  |-- offset: long (nullable = true)
 *  |-- timestamp: timestamp (nullable = true)
 *  |-- timestampType: integer (nullable = true)
 *  |-- headers: array (nullable = true)  // if includeHeaders = true
 * }}}
 *
 * == Value Deserialization ==
 *
 * By default, key and value are returned as binary. To deserialize:
 *
 * {{{
 * // For JSON values
 * val parsed = df.selectExpr(
 *   "CAST(key AS STRING)",
 *   "from_json(CAST(value AS STRING), 'id INT, name STRING') as data"
 * )
 *
 * // For Avro with Schema Registry (requires additional setup)
 * val avroDF = df.select(
 *   from_avro($"value", schemaRegistryUrl).as("data")
 * )
 * }}}
 *
 * == Watermarking ==
 *
 * For event-time processing with late data handling:
 *
 * {{{
 * val config = KafkaSourceConfig(
 *   bootstrapServers = "localhost:9092",
 *   topics = List("events"),
 *   watermarkColumn = Some("timestamp"),
 *   watermarkDelay = Some("10 seconds")
 * )
 * }}}
 *
 * @param config Kafka source configuration
 *
 * @see [[KafkaSourceConfig]] for configuration options
 * @see [[https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html Kafka Integration Guide]]
 */
class KafkaStreamingSource(config: KafkaSourceConfig) extends StreamingSource {

  // Validate configuration on construction
  config.validate()

  override def name: String =
    s"KafkaStreamingSource[${config.topics.mkString(",")}]"

  /**
   * Creates a streaming DataFrame from Kafka topics.
   *
   * @return Streaming DataFrame with Kafka message schema
   */
  override def readStream(): DataFrame = {
    val reader = spark.readStream.format("kafka")

    // Apply all configuration options
    config.toSparkOptions.foreach {
      case (key, value) =>
        reader.option(key, value)
    }

    reader.load()
  }

  /**
   * Returns the watermark column if configured.
   *
   * @return Column name for event time watermarking
   */
  override def watermarkColumn: Option[String] = config.watermarkColumn

  /**
   * Returns the watermark delay if configured.
   *
   * @return Delay threshold for late data
   */
  override def watermarkDelay: Option[String] = config.watermarkDelay
}

/**
 * Factory for creating [[KafkaStreamingSource]] from configuration.
 *
 * == HOCON Configuration Example ==
 *
 * {{{
 * source {
 *   instance-class = "io.github.dwsmith1983.spark.pipeline.streaming.sources.kafka.KafkaStreamingSource"
 *   instance-config {
 *     bootstrap-servers = "localhost:9092"
 *     topics = ["events", "logs"]
 *     starting-offsets = "latest"
 *     watermark-column = "timestamp"
 *     watermark-delay = "10 seconds"
 *
 *     # Optional security
 *     security-protocol = "SASL_SSL"
 *     sasl-mechanism = "SCRAM-SHA-256"
 *     sasl-jaas-config = "org.apache.kafka.common.security.scram.ScramLoginModule required ..."
 *   }
 * }
 * }}}
 */
object KafkaStreamingSource extends ConfigurableInstance {

  /**
   * Internal configuration case class for PureConfig parsing.
   * Uses kebab-case to match HOCON conventions.
   */
  private case class HoconConfig(
    bootstrapServers: String,
    topics: List[String] = List.empty,
    topicPattern: Option[String] = None,
    groupId: Option[String] = None,
    startingOffsets: String = "latest",
    maxOffsetsPerTrigger: Option[Long] = None,
    minPartitions: Option[Int] = None,
    securityProtocol: Option[String] = None,
    saslMechanism: Option[String] = None,
    saslJaasConfig: Option[String] = None,
    sslTruststoreLocation: Option[String] = None,
    sslTruststorePassword: Option[String] = None,
    sslKeystoreLocation: Option[String] = None,
    sslKeystorePassword: Option[String] = None,
    schemaRegistryUrl: Option[String] = None,
    schemaRegistryAuth: Option[String] = None,
    includeHeaders: Boolean = false,
    watermarkColumn: Option[String] = None,
    watermarkDelay: Option[String] = None,
    kafkaOptions: Map[String, String] = Map.empty)

  override def createFromConfig(conf: Config): KafkaStreamingSource = {
    val hoconConfig = ConfigSource.fromConfig(conf).loadOrThrow[HoconConfig]

    val startingOffsets = StartingOffsets.fromString(hoconConfig.startingOffsets)

    val sourceConfig = KafkaSourceConfig(
      bootstrapServers = hoconConfig.bootstrapServers,
      topics = hoconConfig.topics,
      topicPattern = hoconConfig.topicPattern,
      groupId = hoconConfig.groupId,
      startingOffsets = startingOffsets,
      maxOffsetsPerTrigger = hoconConfig.maxOffsetsPerTrigger,
      minPartitions = hoconConfig.minPartitions,
      securityProtocol = hoconConfig.securityProtocol,
      saslMechanism = hoconConfig.saslMechanism,
      saslJaasConfig = hoconConfig.saslJaasConfig,
      sslTruststoreLocation = hoconConfig.sslTruststoreLocation,
      sslTruststorePassword = hoconConfig.sslTruststorePassword,
      sslKeystoreLocation = hoconConfig.sslKeystoreLocation,
      sslKeystorePassword = hoconConfig.sslKeystorePassword,
      schemaRegistryUrl = hoconConfig.schemaRegistryUrl,
      schemaRegistryAuth = hoconConfig.schemaRegistryAuth,
      includeHeaders = hoconConfig.includeHeaders,
      watermarkColumn = hoconConfig.watermarkColumn,
      watermarkDelay = hoconConfig.watermarkDelay,
      kafkaOptions = hoconConfig.kafkaOptions
    )

    new KafkaStreamingSource(sourceConfig)
  }
}
