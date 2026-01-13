package io.github.dwsmith1983.spark.pipeline.streaming.sinks

import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSink
import io.github.dwsmith1983.spark.pipeline.streaming.config.KafkaSinkConfig
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import pureconfig._
import pureconfig.generic.auto._

import java.util.UUID

/**
 * Streaming sink that writes to Apache Kafka with exactly-once semantics.
 *
 * This sink uses Kafka's transactional producer to ensure exactly-once
 * delivery when combined with Spark's checkpointing. Messages are written
 * to the specified topic with optional key and value column mappings.
 *
 * == Exactly-Once Guarantees ==
 *
 * Exactly-once semantics are achieved through:
 * 1. Kafka idempotent producer (`enable.idempotence=true`)
 * 2. Kafka transactions (`transactional.id`)
 * 3. Spark checkpointing (required)
 *
 * == Message Format ==
 *
 * The sink expects the DataFrame to have columns compatible with Kafka:
 *
 * - If `keyColumn` is specified, that column becomes the Kafka key
 * - If `valueColumn` is specified, that column becomes the Kafka value
 * - If neither is specified, all columns are serialized as JSON value
 *
 * == Example ==
 *
 * {{{
 * val config = KafkaSinkConfig(
 *   bootstrapServers = "kafka:9092",
 *   topic = "events",
 *   checkpointPath = "/checkpoints/kafka",
 *   keyColumn = Some("event_id"),
 *   valueColumn = Some("payload")
 * )
 * val sink = new KafkaStreamSink(config)
 * }}}
 *
 * @param config The Kafka sink configuration
 */
class KafkaStreamSink(config: KafkaSinkConfig) extends StreamingSink {

  override def writeStream(df: DataFrame): DataStreamWriter[Row] = {
    val transId = config.transactionalId.getOrElse(
      s"spf-kafka-${config.topic}-${UUID.randomUUID().toString.take(8)}"
    )

    logger.info(
      s"Creating Kafka sink: topic=${config.topic}, " +
        s"servers=${config.bootstrapServers}, " +
        s"transactionalId=$transId, " +
        s"idempotence=${config.enableIdempotence}"
    )

    val preparedDf = prepareDataFrame(df)

    val writer = preparedDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.bootstrapServers)
      .option("topic", config.topic)

    val writerWithTx = if (config.enableIdempotence) {
      writer
        .option("kafka.enable.idempotence", "true")
        .option("kafka.transactional.id", transId)
    } else {
      writer
    }

    config.producerOptions.foldLeft(writerWithTx) { case (w, (k, v)) =>
      w.option(s"kafka.$k", v)
    }
  }

  /**
   * Prepare the DataFrame for Kafka by selecting/creating key and value columns.
   */
  private def prepareDataFrame(df: DataFrame): DataFrame = {
    val valueCol = config.valueColumn match {
      case Some(colName) => col(colName).cast("string").as("value")
      case None          => to_json(struct(df.columns.toIndexedSeq.map(col): _*)).as("value")
    }

    config.keyColumn match {
      case Some(keyColName) =>
        df.select(col(keyColName).cast("string").as("key"), valueCol)
      case None =>
        df.select(valueCol)
    }
  }

  override def outputMode: OutputMode = OutputMode.Append()

  override def checkpointLocation: String = config.checkpointPath

  override def queryName: Option[String] = config.queryName
}

/**
 * Factory for creating KafkaStreamSink instances from HOCON configuration.
 */
object KafkaStreamSink extends ConfigurableInstance {

  override def createFromConfig(conf: com.typesafe.config.Config): KafkaStreamSink =
    new KafkaStreamSink(ConfigSource.fromConfig(conf).loadOrThrow[KafkaSinkConfig])
}
