package io.github.dwsmith1983.spark.pipeline.streaming.sources.kafka

import io.github.dwsmith1983.spark.pipeline.streaming.common.StartingOffsets

/**
 * Configuration for Kafka streaming source.
 *
 * Provides all options needed to connect to and consume from Apache Kafka
 * topics using Spark Structured Streaming.
 *
 * == Basic Usage ==
 *
 * {{{
 * val config = KafkaSourceConfig(
 *   bootstrapServers = "localhost:9092",
 *   topics = List("events", "logs")
 * )
 * }}}
 *
 * == With Authentication ==
 *
 * {{{
 * val config = KafkaSourceConfig(
 *   bootstrapServers = "kafka.example.com:9093",
 *   topics = List("events"),
 *   securityProtocol = Some("SASL_SSL"),
 *   saslMechanism = Some("SCRAM-SHA-256"),
 *   saslJaasConfig = Some("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"pass\";")
 * )
 * }}}
 *
 * == With Schema Registry ==
 *
 * {{{
 * val config = KafkaSourceConfig(
 *   bootstrapServers = "localhost:9092",
 *   topics = List("avro-events"),
 *   schemaRegistryUrl = Some("http://localhost:8081")
 * )
 * }}}
 *
 * @param bootstrapServers   Kafka bootstrap servers (comma-separated)
 * @param topics             List of topics to subscribe to
 * @param topicPattern       Regex pattern for topic subscription (alternative to topics)
 * @param groupId            Consumer group ID (optional, Spark manages offsets)
 * @param startingOffsets    Where to start reading: Earliest, Latest, or Specific
 * @param maxOffsetsPerTrigger Maximum offsets to process per micro-batch
 * @param minPartitions      Minimum partitions to read from Kafka
 * @param securityProtocol   Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
 * @param saslMechanism      SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI
 * @param saslJaasConfig     JAAS configuration for SASL authentication
 * @param sslTruststoreLocation Path to SSL truststore file
 * @param sslTruststorePassword Password for SSL truststore
 * @param sslKeystoreLocation Path to SSL keystore file
 * @param sslKeystorePassword Password for SSL keystore
 * @param schemaRegistryUrl  Confluent Schema Registry URL for Avro deserialization
 * @param schemaRegistryAuth Basic auth credentials for Schema Registry (user:pass)
 * @param includeHeaders     Include Kafka headers in output DataFrame
 * @param watermarkColumn    Column name for watermark (typically "timestamp")
 * @param watermarkDelay     Watermark delay threshold (e.g., "10 seconds")
 * @param kafkaOptions       Additional Kafka consumer options
 *
 * @see [[KafkaStreamingSource]] for the streaming source implementation
 * @see [[https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html Kafka Integration Guide]]
 */
final case class KafkaSourceConfig(
  bootstrapServers: String,
  topics: List[String] = List.empty,
  topicPattern: Option[String] = None,
  groupId: Option[String] = None,
  startingOffsets: StartingOffsets = StartingOffsets.Latest,
  maxOffsetsPerTrigger: Option[Long] = None,
  minPartitions: Option[Int] = None,
  // Security
  securityProtocol: Option[String] = None,
  saslMechanism: Option[String] = None,
  saslJaasConfig: Option[String] = None,
  sslTruststoreLocation: Option[String] = None,
  sslTruststorePassword: Option[String] = None,
  sslKeystoreLocation: Option[String] = None,
  sslKeystorePassword: Option[String] = None,
  // Schema Registry
  schemaRegistryUrl: Option[String] = None,
  schemaRegistryAuth: Option[String] = None,
  // Output options
  includeHeaders: Boolean = false,
  // Watermarking
  watermarkColumn: Option[String] = None,
  watermarkDelay: Option[String] = None,
  // Additional options
  kafkaOptions: Map[String, String] = Map.empty) {

  /**
   * Validates the configuration.
   *
   * @note Throws `IllegalArgumentException` if configuration is invalid
   */
  def validate(): Unit = {
    require(bootstrapServers.nonEmpty, "bootstrapServers cannot be empty")
    require(
      topics.nonEmpty || topicPattern.isDefined,
      "Either topics or topicPattern must be specified"
    )
    require(
      topics.isEmpty || topicPattern.isEmpty,
      "Cannot specify both topics and topicPattern"
    )

    // Validate security settings
    securityProtocol.foreach { protocol =>
      val validProtocols = Set("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")
      require(
        validProtocols.contains(protocol.toUpperCase),
        s"Invalid securityProtocol: $protocol. Must be one of: ${validProtocols.mkString(", ")}"
      )
    }

    saslMechanism.foreach { mechanism =>
      val validMechanisms = Set("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", "OAUTHBEARER")
      require(
        validMechanisms.contains(mechanism.toUpperCase),
        s"Invalid saslMechanism: $mechanism. Must be one of: ${validMechanisms.mkString(", ")}"
      )
    }
  }

  /**
   * Converts this configuration to Spark DataFrame reader options.
   *
   * @return Map of option name to value
   */
  def toSparkOptions: Map[String, String] = {
    val baseOptions = Map(
      "kafka.bootstrap.servers" -> bootstrapServers,
      "startingOffsets"         -> startingOffsets.toKafkaString
    )

    val topicOptions =
      if (topics.nonEmpty) Map("subscribe" -> topics.mkString(","))
      else topicPattern.map(p => Map("subscribePattern" -> p)).getOrElse(Map.empty)

    val optionalOptions = Map(
      "kafka.group.id"                -> groupId,
      "maxOffsetsPerTrigger"          -> maxOffsetsPerTrigger.map(_.toString),
      "minPartitions"                 -> minPartitions.map(_.toString),
      "kafka.security.protocol"       -> securityProtocol,
      "kafka.sasl.mechanism"          -> saslMechanism,
      "kafka.sasl.jaas.config"        -> saslJaasConfig,
      "kafka.ssl.truststore.location" -> sslTruststoreLocation,
      "kafka.ssl.truststore.password" -> sslTruststorePassword,
      "kafka.ssl.keystore.location"   -> sslKeystoreLocation,
      "kafka.ssl.keystore.password"   -> sslKeystorePassword,
      "includeHeaders"                -> Some(includeHeaders.toString)
    ).collect { case (k, Some(v)) => k -> v }

    baseOptions ++ topicOptions ++ optionalOptions ++ kafkaOptions
  }
}

/** Companion object for [[KafkaSourceConfig]]. */
object KafkaSourceConfig {

  /**
   * Creates a minimal configuration for local development.
   *
   * @param topics Topics to subscribe to
   * @return Configuration for localhost:9092
   */
  def local(topics: String*): KafkaSourceConfig =
    KafkaSourceConfig(
      bootstrapServers = "localhost:9092",
      topics = topics.toList,
      startingOffsets = StartingOffsets.Latest
    )

  /**
   * Creates a configuration with SASL/SCRAM authentication.
   *
   * @param bootstrapServers Kafka bootstrap servers
   * @param topics           Topics to subscribe to
   * @param username         SASL username
   * @param password         SASL password
   * @param useSsl           Whether to use SSL
   * @return Configured KafkaSourceConfig
   */
  def withScramAuth(
    bootstrapServers: String,
    topics: List[String],
    username: String,
    password: String,
    useSsl: Boolean = true
  ): KafkaSourceConfig = {
    val jaasConfig =
      s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$username" password="$password";"""

    KafkaSourceConfig(
      bootstrapServers = bootstrapServers,
      topics = topics,
      securityProtocol = Some(if (useSsl) "SASL_SSL" else "SASL_PLAINTEXT"),
      saslMechanism = Some("SCRAM-SHA-256"),
      saslJaasConfig = Some(jaasConfig)
    )
  }
}
