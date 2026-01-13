package io.github.dwsmith1983.spark.pipeline.streaming.sources.eventhubs

import com.typesafe.config.Config
import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSource
import org.apache.spark.sql.DataFrame
import pureconfig._
import pureconfig.generic.auto._

/**
 * Streaming source for Azure Event Hubs.
 *
 * Reads data from Event Hubs using the Spark Event Hubs connector.
 * Supports connection string authentication and managed identity.
 *
 * == Basic Usage (Connection String) ==
 *
 * {{{
 * val config = EventHubsSourceConfig(
 *   connectionString = "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=...",
 *   eventHubName = "my-hub",
 *   consumerGroup = "$Default"
 * )
 * val source = new EventHubsStreamingSource(config)
 * val df = source.readStream()
 * }}}
 *
 * == With Managed Identity ==
 *
 * {{{
 * val config = EventHubsSourceConfig(
 *   fullyQualifiedNamespace = "mynamespace.servicebus.windows.net",
 *   eventHubName = "my-hub",
 *   useManagedIdentity = true
 * )
 * }}}
 *
 * == Output Schema ==
 *
 * The output DataFrame includes:
 * {{{
 * root
 *  |-- body: binary (the event payload)
 *  |-- partition: string
 *  |-- offset: string
 *  |-- sequenceNumber: long
 *  |-- enqueuedTime: timestamp
 *  |-- publisher: string
 *  |-- partitionKey: string
 *  |-- properties: map<string,string>
 *  |-- systemProperties: map<string,string>
 * }}}
 *
 * == Dependencies ==
 *
 * Requires the Azure Event Hubs Spark connector:
 * {{{
 * "com.azure" % "azure-eventhubs-spark" % "x.y.z"
 * }}}
 *
 * @param config Event Hubs source configuration
 *
 * @see [[EventHubsSourceConfig]] for configuration options
 * @see [[https://github.com/Azure/azure-event-hubs-spark Azure Event Hubs Connector]]
 */
class EventHubsStreamingSource(config: EventHubsSourceConfig) extends StreamingSource {

  config.validate()

  override def name: String =
    s"EventHubsStreamingSource[${config.eventHubName}]"

  /**
   * Creates a streaming DataFrame from Event Hubs.
   *
   * @return Streaming DataFrame with Event Hubs message schema
   */
  override def readStream(): DataFrame = {
    val reader = spark.readStream.format("eventhubs")

    // Connection method: connection string or managed identity
    config.connectionString match {
      case Some(connStr) =>
        reader.option("eventhubs.connectionString", connStr)
      case None if config.useManagedIdentity =>
        config.fullyQualifiedNamespace.foreach(ns => reader.option("eventhubs.namespace", ns))
        reader.option("eventhubs.useManagedIdentity", "true")
      case _ =>
        throw new IllegalStateException(
          "Either connectionString or useManagedIdentity with fullyQualifiedNamespace must be configured"
        )
    }

    // Apply Event Hub name and consumer group
    reader.option("eventhubs.eventHubName", config.eventHubName)
    reader.option("eventhubs.consumerGroup", config.consumerGroup)

    // Apply starting position
    reader.option("eventhubs.startingPosition", config.startingPosition.toJson)

    // Apply performance options
    config.maxEventsPerTrigger.foreach(n => reader.option("eventhubs.maxEventsPerTrigger", n))
    config.receiverTimeout.foreach(t => reader.option("eventhubs.receiverTimeout", t))
    config.operationTimeout.foreach(t => reader.option("eventhubs.operationTimeout", t))

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

/** Starting position for Event Hubs streaming. */
sealed trait EventHubsStartingPosition {
  def toJson: String
}

object EventHubsStartingPosition {

  case object StartOfStream extends EventHubsStartingPosition {
    override def toJson: String = """{"offset":"-1","isInclusive":true}"""
  }

  case object EndOfStream extends EventHubsStartingPosition {
    override def toJson: String = """{"offset":"@latest"}"""
  }

  final case class FromEnqueuedTime(epochMs: Long) extends EventHubsStartingPosition {
    override def toJson: String = s"""{"enqueuedTime":"$epochMs"}"""
  }

  final case class FromOffset(offset: String, inclusive: Boolean = true) extends EventHubsStartingPosition {
    override def toJson: String = s"""{"offset":"$offset","isInclusive":$inclusive}"""
  }

  def fromString(s: String): EventHubsStartingPosition = s.toLowerCase match {
    case "earliest" | "start"             => StartOfStream
    case "latest" | "end"                 => EndOfStream
    case ts if ts.startsWith("enqueued:") => FromEnqueuedTime(ts.stripPrefix("enqueued:").toLong)
    case off if off.startsWith("offset:") => FromOffset(off.stripPrefix("offset:"))
    case other =>
      throw new IllegalArgumentException(s"Invalid Event Hubs starting position: $other")
  }
}

/**
 * Configuration for Event Hubs streaming source.
 *
 * @param eventHubName            Event Hub name
 * @param connectionString        Connection string (for SAS auth)
 * @param fullyQualifiedNamespace Namespace for managed identity auth
 * @param useManagedIdentity      Use Azure managed identity
 * @param consumerGroup           Consumer group name
 * @param startingPosition        Where to start reading
 * @param maxEventsPerTrigger     Max events per micro-batch
 * @param receiverTimeout         Receiver timeout duration
 * @param operationTimeout        Operation timeout duration
 * @param watermarkColumn         Column for watermark
 * @param watermarkDelay          Watermark delay threshold
 * @param options                 Additional connector options
 */
final case class EventHubsSourceConfig(
  eventHubName: String,
  connectionString: Option[String] = None,
  fullyQualifiedNamespace: Option[String] = None,
  useManagedIdentity: Boolean = false,
  consumerGroup: String = "$Default",
  startingPosition: EventHubsStartingPosition = EventHubsStartingPosition.EndOfStream,
  maxEventsPerTrigger: Option[Long] = None,
  receiverTimeout: Option[String] = None,
  operationTimeout: Option[String] = None,
  watermarkColumn: Option[String] = None,
  watermarkDelay: Option[String] = None,
  options: Map[String, String] = Map.empty) {

  def validate(): Unit = {
    require(eventHubName.nonEmpty, "eventHubName cannot be empty")
    require(
      connectionString.isDefined || (useManagedIdentity && fullyQualifiedNamespace.isDefined),
      "Either connectionString or (useManagedIdentity with fullyQualifiedNamespace) must be specified"
    )
  }
}

/**
 * Factory for creating [[EventHubsStreamingSource]] from configuration.
 *
 * == HOCON Configuration Example (Connection String) ==
 *
 * {{{
 * source {
 *   instance-class = "io.github.dwsmith1983.spark.pipeline.streaming.sources.eventhubs.EventHubsStreamingSource"
 *   instance-config {
 *     event-hub-name = "my-hub"
 *     connection-string = "Endpoint=sb://..."
 *     consumer-group = "$Default"
 *     starting-position = "latest"
 *     max-events-per-trigger = 10000
 *   }
 * }
 * }}}
 *
 * == HOCON Configuration Example (Managed Identity) ==
 *
 * {{{
 * source {
 *   instance-class = "io.github.dwsmith1983.spark.pipeline.streaming.sources.eventhubs.EventHubsStreamingSource"
 *   instance-config {
 *     event-hub-name = "my-hub"
 *     fully-qualified-namespace = "mynamespace.servicebus.windows.net"
 *     use-managed-identity = true
 *     consumer-group = "$Default"
 *   }
 * }
 * }}}
 */
object EventHubsStreamingSource extends ConfigurableInstance {

  private case class HoconConfig(
    eventHubName: String,
    connectionString: Option[String] = None,
    fullyQualifiedNamespace: Option[String] = None,
    useManagedIdentity: Boolean = false,
    consumerGroup: String = "$Default",
    startingPosition: String = "latest",
    maxEventsPerTrigger: Option[Long] = None,
    receiverTimeout: Option[String] = None,
    operationTimeout: Option[String] = None,
    watermarkColumn: Option[String] = None,
    watermarkDelay: Option[String] = None,
    options: Map[String, String] = Map.empty)

  override def createFromConfig(conf: Config): EventHubsStreamingSource = {
    val hoconConfig = ConfigSource.fromConfig(conf).loadOrThrow[HoconConfig]

    val config = EventHubsSourceConfig(
      eventHubName = hoconConfig.eventHubName,
      connectionString = hoconConfig.connectionString,
      fullyQualifiedNamespace = hoconConfig.fullyQualifiedNamespace,
      useManagedIdentity = hoconConfig.useManagedIdentity,
      consumerGroup = hoconConfig.consumerGroup,
      startingPosition = EventHubsStartingPosition.fromString(hoconConfig.startingPosition),
      maxEventsPerTrigger = hoconConfig.maxEventsPerTrigger,
      receiverTimeout = hoconConfig.receiverTimeout,
      operationTimeout = hoconConfig.operationTimeout,
      watermarkColumn = hoconConfig.watermarkColumn,
      watermarkDelay = hoconConfig.watermarkDelay,
      options = hoconConfig.options
    )

    new EventHubsStreamingSource(config)
  }
}
