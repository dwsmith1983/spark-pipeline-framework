package io.github.dwsmith1983.spark.pipeline.streaming.config

/**
 * Configuration for Kafka streaming sink with exactly-once semantics.
 *
 * This config supports Kafka's transactional producer for exactly-once
 * delivery guarantees when used with Spark Structured Streaming checkpointing.
 *
 * == Exactly-Once Semantics ==
 *
 * To achieve exactly-once delivery:
 * 1. Set `enableIdempotence = true` (default)
 * 2. Provide a unique `transactionalId` or let the framework generate one
 * 3. Ensure checkpointing is enabled (required by Spark)
 *
 * == Message Format ==
 *
 * The DataFrame must contain columns that can be serialized to Kafka:
 * - `key` (optional): Message key (String or Binary)
 * - `value` (required): Message value (String or Binary)
 * - `headers` (optional): Message headers
 *
 * If your DataFrame has different column names, use `keyColumn` and
 * `valueColumn` to specify which columns to use.
 *
 * == Example HOCON ==
 *
 * {{{
 * instance-config {
 *   bootstrap-servers = "kafka1:9092,kafka2:9092"
 *   topic = "events"
 *   checkpoint-path = "/checkpoints/kafka-sink"
 *   enable-idempotence = true
 *   key-column = "event_id"
 *   value-column = "payload"
 * }
 * }}}
 *
 * @param bootstrapServers Kafka broker addresses (comma-separated)
 * @param topic Target Kafka topic
 * @param checkpointPath Path for Spark checkpoint storage
 * @param transactionalId Unique ID for Kafka transactions (auto-generated if not set)
 * @param enableIdempotence Enable idempotent producer for exactly-once (default: true)
 * @param keyColumn DataFrame column to use as Kafka message key
 * @param valueColumn DataFrame column to use as Kafka message value
 * @param queryName Optional name for the streaming query
 * @param producerOptions Additional Kafka producer configuration
 */
case class KafkaSinkConfig(
  bootstrapServers: String,
  topic: String,
  checkpointPath: String,
  transactionalId: Option[String] = None,
  enableIdempotence: Boolean = true,
  keyColumn: Option[String] = None,
  valueColumn: Option[String] = None,
  queryName: Option[String] = None,
  producerOptions: Map[String, String] = Map.empty)
