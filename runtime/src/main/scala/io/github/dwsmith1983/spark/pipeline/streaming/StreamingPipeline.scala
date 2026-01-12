package io.github.dwsmith1983.spark.pipeline.streaming

import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * Orchestrates streaming pipelines by connecting sources to sinks.
 *
 * StreamingPipeline is the main abstraction for building Spark Structured
 * Streaming applications. It connects a [[StreamingSource]] to a
 * [[StreamingSink]], optionally applying transformations in between.
 *
 * == Pipeline Flow ==
 *
 * {{{
 * Source.readStream() → transform() → Sink.writeStream() → start()
 * }}}
 *
 * == Usage ==
 *
 * There are two ways to execute a streaming pipeline:
 *
 * '''1. startStream()''' - Returns immediately with a StreamingQuery handle.
 * Use this when you need to manage query lifecycle manually:
 *
 * {{{
 * val query = pipeline.startStream()
 * // Do other work...
 * query.awaitTermination()
 * }}}
 *
 * '''2. run()''' - Blocks until the query terminates. Use this for
 * simple standalone streaming applications:
 *
 * {{{
 * pipeline.run() // Blocks forever (or until error/stop)
 * }}}
 *
 * == Watermarking ==
 *
 * If the source defines [[StreamingSource.watermarkColumn]] and
 * [[StreamingSource.watermarkDelay]], watermarking is automatically
 * applied to the DataFrame before transformations.
 *
 * == Example ==
 *
 * {{{
 * import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
 * import pureconfig._
 * import pureconfig.generic.auto._
 *
 * object MyStreamingPipeline extends ConfigurableInstance {
 *   case class Config(
 *     kafkaBootstrapServers: String,
 *     kafkaTopic: String,
 *     outputPath: String,
 *     checkpointPath: String,
 *     triggerInterval: String = "10 seconds"
 *   )
 *
 *   override def createFromConfig(conf: com.typesafe.config.Config): MyStreamingPipeline =
 *     new MyStreamingPipeline(ConfigSource.fromConfig(conf).loadOrThrow[Config])
 * }
 *
 * class MyStreamingPipeline(conf: MyStreamingPipeline.Config) extends StreamingPipeline {
 *
 *   override def source: StreamingSource = new StreamingSource {
 *     override def readStream(): DataFrame =
 *       spark.readStream
 *         .format("kafka")
 *         .option("kafka.bootstrap.servers", conf.kafkaBootstrapServers)
 *         .option("subscribe", conf.kafkaTopic)
 *         .load()
 *   }
 *
 *   override def sink: StreamingSink = new StreamingSink {
 *     override def writeStream(df: DataFrame) =
 *       df.writeStream.format("parquet").option("path", conf.outputPath)
 *
 *     override def outputMode = org.apache.spark.sql.streaming.OutputMode.Append()
 *     override def checkpointLocation = conf.checkpointPath
 *     override def queryName = Some("my-streaming-pipeline")
 *   }
 *
 *   override def transform(df: DataFrame): DataFrame = {
 *     import spark.implicits._
 *     df.selectExpr("CAST(value AS STRING) as message")
 *       .filter($"message".isNotNull)
 *   }
 *
 *   override def trigger = TriggerConfig.ProcessingTime(conf.triggerInterval)
 * }
 * }}}
 *
 * @see [[StreamingSource]] for reading streaming data
 * @see [[StreamingSink]] for writing streaming data
 * @see `TriggerConfig` for trigger configuration options
 */
trait StreamingPipeline extends DataFlow {

  /**
   * The streaming source to read from.
   *
   * @return A StreamingSource that provides the input DataFrame
   */
  def source: StreamingSource

  /**
   * The streaming sink to write to.
   *
   * @return A StreamingSink that handles output
   */
  def sink: StreamingSink

  /**
   * Optional transformation to apply between source and sink.
   *
   * Override this method to apply filtering, projection, aggregation,
   * or other DataFrame operations. The default implementation returns
   * the input DataFrame unchanged (identity transformation).
   *
   * @param df The streaming DataFrame from the source
   * @return The transformed streaming DataFrame
   */
  def transform(df: DataFrame): DataFrame = df

  /**
   * The trigger configuration for this pipeline.
   *
   * Override to customize how often micro-batches are executed.
   * Default is ProcessingTime with 0 seconds (as fast as possible).
   *
   * @return The trigger configuration
   */
  def trigger: TriggerConfig = TriggerConfig.ProcessingTime("0 seconds")

  /**
   * Starts the streaming query and returns a handle for management.
   *
   * This method:
   * 1. Reads from the source
   * 2. Applies watermarking (if configured)
   * 3. Applies transformations
   * 4. Configures the sink with output mode, trigger, and checkpoint
   * 5. Starts the query
   *
   * @return A StreamingQuery handle for monitoring and control
   */
  def startStream(): StreamingQuery = {
    logger.info(s"Starting streaming pipeline: $name")
    logger.info(s"Source: ${source.name}, Sink: ${sink.name}")
    logger.info(s"Trigger: ${trigger.description}")
    logger.info(s"Checkpoint: ${sink.checkpointLocation}")

    // Read from source
    var df = source.readStream()

    // Apply watermarking if configured
    (source.watermarkColumn, source.watermarkDelay) match {
      case (Some(column), Some(delay)) =>
        logger.info(s"Applying watermark: column=$column, delay=$delay")
        df = df.withWatermark(column, delay)
      case _ => // No watermarking
    }

    // Apply transformations
    val transformedDF = transform(df)

    // Configure writer
    var writer = sink.writeStream(transformedDF)
      .outputMode(sink.outputMode)
      .trigger(TriggerConverter.toSparkTrigger(trigger))
      .option("checkpointLocation", sink.checkpointLocation)

    // Set query name if provided
    sink.queryName.foreach { qn =>
      writer = writer.queryName(qn)
      logger.info(s"Query name: $qn")
    }

    // Start the query
    val query = writer.start()
    logger.info(s"Streaming query started: id=${query.id}, runId=${query.runId}")

    query
  }

  /**
   * Runs the streaming pipeline and blocks until termination.
   *
   * This is a convenience method equivalent to:
   * {{{
   * startStream().awaitTermination()
   * }}}
   *
   * The method will block until:
   * - The query is manually stopped via `query.stop()`
   * - An exception occurs
   * - For Once/AvailableNow triggers: all data is processed
   */
  override def run(): Unit = {
    val query = startStream()
    logger.info("Awaiting termination...")
    query.awaitTermination()
    logger.info("Streaming query terminated")
  }
}
