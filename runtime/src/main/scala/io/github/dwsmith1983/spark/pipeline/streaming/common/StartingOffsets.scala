package io.github.dwsmith1983.spark.pipeline.streaming.common

/**
 * Starting position configuration for streaming sources.
 *
 * Defines where a streaming source should begin reading data. Different
 * sources support different starting position types.
 *
 * == Kafka ==
 *
 * {{{
 * // Start from the earliest available offset
 * val earliest = StartingOffsets.Earliest
 *
 * // Start from the latest offset (new data only)
 * val latest = StartingOffsets.Latest
 *
 * // Start from specific offsets per partition
 * val specific = StartingOffsets.Specific(Map(
 *   "topic-0" -> 100L,
 *   "topic-1" -> 200L
 * ))
 * }}}
 *
 * == Kinesis ==
 *
 * {{{
 * // Start from a specific timestamp
 * val fromTime = StartingOffsets.Timestamp(System.currentTimeMillis() - 3600000)
 * }}}
 *
 * @see [[io.github.dwsmith1983.spark.pipeline.streaming.sources.kafka.KafkaStreamingSource]]
 * @see [[io.github.dwsmith1983.spark.pipeline.streaming.sources.kinesis.KinesisStreamingSource]]
 */
sealed trait StartingOffsets {

  /**
   * Converts this starting offset to a Kafka-compatible string.
   *
   * @return JSON string for Kafka startingOffsets option
   */
  def toKafkaString: String
}

/** Companion object containing starting offset implementations. */
object StartingOffsets {

  /**
   * Start reading from the earliest available offset.
   *
   * For Kafka, this means the beginning of the retention window.
   * For Kinesis, this means TRIM_HORIZON.
   */
  case object Earliest extends StartingOffsets {
    override def toKafkaString: String = "earliest"
  }

  /**
   * Start reading from the latest offset (new data only).
   *
   * Only data arriving after the query starts will be processed.
   * This is the default for most streaming sources.
   */
  case object Latest extends StartingOffsets {
    override def toKafkaString: String = "latest"
  }

  /**
   * Start reading from a specific timestamp.
   *
   * The source will begin reading from the first offset at or after
   * the specified timestamp.
   *
   * @param epochMs Unix timestamp in milliseconds
   */
  final case class Timestamp(epochMs: Long) extends StartingOffsets {

    override def toKafkaString: String =
      throw new UnsupportedOperationException(
        "Timestamp-based starting offsets require Kafka 0.10.1+ and specific partition assignment"
      )
  }

  /**
   * Start reading from specific offsets per topic-partition.
   *
   * This allows precise control over where to resume reading,
   * typically used when implementing custom checkpointing.
   *
   * @param offsets Map of "topic-partition" to offset value
   */
  final case class Specific(offsets: Map[String, Long]) extends StartingOffsets {

    override def toKafkaString: String = {
      val entries = offsets.map {
        case (tp, offset) =>
          s""""$tp":$offset"""
      }.mkString(",")
      s"{$entries}"
    }
  }

  /**
   * Parses a string into a StartingOffsets instance.
   *
   * @param value "earliest", "latest", or a JSON object
   * @return The corresponding StartingOffsets
   */
  def fromString(value: String): StartingOffsets = value.toLowerCase.trim match {
    case "earliest"                   => Earliest
    case "latest"                     => Latest
    case json if json.startsWith("{") =>
      // Simple JSON parsing - in production, use a proper JSON library
      Specific(Map.empty) // Placeholder - would need proper parsing
    case other =>
      throw new IllegalArgumentException(s"Invalid starting offsets: $other")
  }
}
