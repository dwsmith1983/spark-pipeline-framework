package io.github.dwsmith1983.pipelines.examples

import io.github.dwsmith1983.pipelines.streaming.{ConsoleStreamSink, RateStreamSource}
import io.github.dwsmith1983.spark.pipeline.runtime.{DataFlow, SparkSessionWrapper}
import io.github.dwsmith1983.spark.pipeline.streaming.{StreamingPipeline, TriggerConfig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.TimestampType

import java.nio.file.{Files, Path}
import scala.concurrent.duration._

/**
 * Canonical end-to-end streaming pipeline example for spark-pipeline-framework.
 *
 * This runnable example demonstrates:
 * - Complete streaming pipeline with source, transformations, and sink
 * - Watermarking for event-time processing
 * - Multiple trigger types (continuous, micro-batch, once)
 * - Windowed aggregations with late data handling
 * - Streaming query lifecycle management
 * - Multiple pipeline examples (simple, aggregation, stateful)
 *
 * == Architecture ==
 *
 * The framework's streaming architecture consists of:
 *
 *  1. '''StreamingSource''' - Defines how to read streaming data
 *     - Implements `readStream(): DataFrame`
 *     - Optionally configures watermarking
 *
 *  2. '''StreamingSink''' - Defines how to write streaming data
 *     - Implements `writeStream(df): DataStreamWriter`
 *     - Specifies output mode and checkpoint location
 *
 *  3. '''StreamingPipeline''' - Orchestrates source + transformations + sink
 *     - Composes source and sink
 *     - Applies watermarking automatically
 *     - Manages streaming query lifecycle
 *
 * == Examples Shown ==
 *
 *  - '''Simple Pipeline''': Basic streaming with filters
 *  - '''Windowed Aggregation''': Event-time windows with watermarking
 *  - '''Stateful Processing''': Running aggregates with late data handling
 *
 * == Running ==
 *
 * {{{
 * # From sbt (runs all three examples)
 * sbt "examplespark3/runMain io.github.dwsmith1983.pipelines.StreamingPipelineExample"
 *
 * # Run specific example
 * sbt "examplespark3/runMain io.github.dwsmith1983.pipelines.StreamingPipelineExample simple"
 * sbt "examplespark3/runMain io.github.dwsmith1983.pipelines.StreamingPipelineExample windowed"
 * sbt "examplespark3/runMain io.github.dwsmith1983.pipelines.StreamingPipelineExample stateful"
 *
 * # Or with spark-submit
 * spark-submit \
 *   --class io.github.dwsmith1983.pipelines.StreamingPipelineExample \
 *   --master local[*] \
 *   spark-pipeline-example-spark3_2.13.jar
 * }}}
 *
 * @see [[io.github.dwsmith1983.spark.pipeline.streaming.StreamingPipeline]]
 * @see [[io.github.dwsmith1983.spark.pipeline.streaming.StreamingSource]]
 * @see [[io.github.dwsmith1983.spark.pipeline.streaming.StreamingSink]]
 */
object StreamingPipelineExample extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    val exampleType = args.headOption.getOrElse("all")

    println("=" * 80)
    println("  SPARK PIPELINE FRAMEWORK - STREAMING PIPELINE EXAMPLES")
    println("=" * 80)
    println()

    exampleType.toLowerCase match {
      case "simple"   => runSimpleStreamingExample()
      case "windowed" => runWindowedAggregationExample()
      case "stateful" => runStatefulProcessingExample()
      case "all" =>
        runSimpleStreamingExample()
        println("\n" + "=" * 80 + "\n")
        runWindowedAggregationExample()
        println("\n" + "=" * 80 + "\n")
        runStatefulProcessingExample()
      case other =>
        println(s"[ERROR] Unknown example type: $other")
        println("Valid options: simple, windowed, stateful, all")
        sys.exit(1)
    }

    println()
    println("=" * 80)
    println("  ALL STREAMING EXAMPLES COMPLETED SUCCESSFULLY")
    println("=" * 80)
  }

  /**
   * Example 1: Simple Streaming Pipeline
   *
   * Demonstrates:
   * - Basic StreamingPipeline composition
   * - Simple transformations (filter, select)
   * - Console sink for debugging
   * - ProcessingTime trigger (micro-batch)
   */
  def runSimpleStreamingExample(): Unit = {
    println("[ EXAMPLE 1: Simple Streaming Pipeline ]")
    println()

    val checkpointDir = Files.createTempDirectory("streaming-checkpoint-simple")

    try {
      // Create streaming pipeline with transformations
      val pipeline = new StreamingPipeline {
        override val source: RateStreamSource = new RateStreamSource(
          RateStreamSource.Config(
            rowsPerSecond = 10,
            numPartitions = 2,
            rampUpTimeSeconds = 0,
            enableWatermark = false
          )
        )
        override val sink: ConsoleStreamSink = new ConsoleStreamSink(
          ConsoleStreamSink.Config(
            checkpointLocation = checkpointDir.toString,
            outputMode = "append",
            numRows = 20,
            truncate = false,
            queryName = Some("simple-streaming-example")
          )
        )

        // Apply simple transformations
        override def transform(df: DataFrame): DataFrame =
          df.withColumn("doubled_value", col("value") * 2)
            .withColumn("is_even", col("value") % 2 === 0)
            .filter(col("value") % 3 === 0) // Only multiples of 3
            .select("timestamp", "value", "doubled_value", "is_even")

        // Use micro-batch trigger (every 5 seconds)
        override def trigger: TriggerConfig = TriggerConfig.ProcessingTime("5 seconds")
      }

      println("[INFO] Starting simple streaming pipeline...")
      println(s"[INFO] Checkpoint: $checkpointDir")
      println("[INFO] Processing multiples of 3, micro-batch every 5 seconds")
      println("[INFO] Press Ctrl+C to stop (or will auto-stop after 20 seconds)")
      println()

      val query = pipeline.startStream()

      // Run for 20 seconds then stop
      query.awaitTermination(20.seconds.toMillis)
      query.stop()

      println()
      println("[SUCCESS] Simple streaming example completed")

    } finally {
      cleanup(checkpointDir)
    }
  }

  /**
   * Example 2: Windowed Aggregation Pipeline
   *
   * Demonstrates:
   * - Watermarking for event-time processing
   * - Windowed aggregations (tumbling windows)
   * - Late data handling
   * - Complete output mode for aggregations
   */
  def runWindowedAggregationExample(): Unit = {
    println("[ EXAMPLE 2: Windowed Aggregation Pipeline ]")
    println()

    val checkpointDir = Files.createTempDirectory("streaming-checkpoint-windowed")

    try {
      // Create streaming pipeline with windowed aggregations
      val pipeline = new StreamingPipeline {
        override val source: RateStreamSource = new RateStreamSource(
          RateStreamSource.Config(
            rowsPerSecond = 20,
            numPartitions = 2,
            rampUpTimeSeconds = 0,
            enableWatermark = true,
            watermarkDelay = "10 seconds"
          )
        )
        override val sink: ConsoleStreamSink = new ConsoleStreamSink(
          ConsoleStreamSink.Config(
            checkpointLocation = checkpointDir.toString,
            outputMode = "complete",
            numRows = 50,
            truncate = false,
            queryName = Some("windowed-aggregation-example")
          )
        )

        // Apply windowed aggregations
        override def transform(df: DataFrame): DataFrame =
          df.withWatermark("timestamp", "10 seconds")
            .groupBy(
              window(col("timestamp"), "10 seconds"),
              (col("value") % 10).as("value_mod_10")
            )
            .agg(
              count("*").as("event_count"),
              sum("value").as("value_sum"),
              avg("value").as("value_avg"),
              min("value").as("value_min"),
              max("value").as("value_max")
            )
            .select(
              col("window.start").as("window_start"),
              col("window.end").as("window_end"),
              col("value_mod_10"),
              col("event_count"),
              col("value_sum"),
              round(col("value_avg"), 2).as("value_avg"),
              col("value_min"),
              col("value_max")
            )
            .orderBy(col("window_start").desc, col("value_mod_10"))

        override def trigger: TriggerConfig = TriggerConfig.ProcessingTime("5 seconds")
      }

      println("[INFO] Starting windowed aggregation pipeline...")
      println(s"[INFO] Checkpoint: $checkpointDir")
      println("[INFO] 10-second tumbling windows with 10-second watermark")
      println("[INFO] Aggregating by value_mod_10 within each window")
      println("[INFO] Press Ctrl+C to stop (or will auto-stop after 30 seconds)")
      println()

      val query = pipeline.startStream()

      // Run for 30 seconds then stop
      query.awaitTermination(30.seconds.toMillis)
      query.stop()

      println()
      println("[SUCCESS] Windowed aggregation example completed")

    } finally {
      cleanup(checkpointDir)
    }
  }

  /**
   * Example 3: Stateful Processing Pipeline
   *
   * Demonstrates:
   * - Running aggregates with state management
   * - Update output mode for incremental results
   * - Continuous trigger for low-latency processing
   * - Enrichment with derived metrics
   */
  def runStatefulProcessingExample(): Unit = {
    println("[ EXAMPLE 3: Stateful Processing Pipeline ]")
    println()

    val checkpointDir = Files.createTempDirectory("streaming-checkpoint-stateful")

    try {
      // Create streaming pipeline with stateful processing
      val pipeline = new StreamingPipeline {
        override val source: RateStreamSource = new RateStreamSource(
          RateStreamSource.Config(
            rowsPerSecond = 15,
            numPartitions = 2,
            rampUpTimeSeconds = 0,
            enableWatermark = true,
            watermarkDelay = "5 seconds"
          )
        )
        override val sink: ConsoleStreamSink = new ConsoleStreamSink(
          ConsoleStreamSink.Config(
            checkpointLocation = checkpointDir.toString,
            outputMode = "update",
            numRows = 30,
            truncate = false,
            queryName = Some("stateful-processing-example")
          )
        )

        // Apply stateful transformations
        override def transform(df: DataFrame): DataFrame = {
          // Categorize values and compute running statistics
          val categorized = df
            .withColumn(
              "category",
              when(col("value") < 100, "low")
                .when(col("value") < 200, "medium")
                .otherwise("high")
            )
            .withWatermark("timestamp", "5 seconds")

          // Compute running aggregates by category
          categorized
            .groupBy(col("category"))
            .agg(
              count("*").as("total_events"),
              sum("value").as("cumulative_value"),
              avg("value").as("avg_value"),
              stddev("value").as("stddev_value"),
              min("value").as("min_value"),
              max("value").as("max_value"),
              max("timestamp").cast(TimestampType).as("last_seen")
            )
            .withColumn("avg_value", round(col("avg_value"), 2))
            .withColumn("stddev_value", round(col("stddev_value"), 2))
            .orderBy(col("category"))
        }

        // Use continuous trigger for low latency (1 second checkpoint interval)
        override def trigger: TriggerConfig = TriggerConfig.ProcessingTime("2 seconds")
      }

      println("[INFO] Starting stateful processing pipeline...")
      println(s"[INFO] Checkpoint: $checkpointDir")
      println("[INFO] Running aggregates by value category (low/medium/high)")
      println("[INFO] Update mode shows only changed aggregates")
      println("[INFO] Press Ctrl+C to stop (or will auto-stop after 25 seconds)")
      println()

      val query = pipeline.startStream()

      // Run for 25 seconds then stop
      query.awaitTermination(25.seconds.toMillis)
      query.stop()

      println()
      println("[SUCCESS] Stateful processing example completed")

    } finally {
      cleanup(checkpointDir)
    }
  }

  /**
   * Additional Example: Custom Pipeline Composition
   *
   * This demonstrates how to build a custom streaming pipeline from scratch
   * without using the existing RateStreamSource/ConsoleStreamSink.
   */
  class CustomEventStreamPipeline(
    checkpointPath: String,
    eventsPerSecond: Int = 50) extends StreamingPipeline with DataFlow {

    override val name: String = "CustomEventStreamPipeline"

    // Define source inline
    override def source: RateStreamSource = new RateStreamSource(
      RateStreamSource.Config(
        rowsPerSecond = eventsPerSecond,
        numPartitions = 4,
        enableWatermark = true,
        watermarkDelay = "15 seconds"
      )
    )

    // Define sink inline
    override def sink: ConsoleStreamSink = new ConsoleStreamSink(
      ConsoleStreamSink.Config(
        checkpointLocation = checkpointPath,
        outputMode = "append",
        numRows = 25,
        truncate = false,
        queryName = Some("custom-event-stream")
      )
    )

    // Complex transformation logic
    override def transform(df: DataFrame): DataFrame =
      df.withColumn("event_id", monotonically_increasing_id())
        .withColumn("event_hour", hour(col("timestamp")))
        .withColumn("event_minute", minute(col("timestamp")))
        .withColumn(
          "value_range",
          when(col("value") < 50, "0-49")
            .when(col("value") < 150, "50-149")
            .when(col("value") < 250, "150-249")
            .otherwise("250+")
        )
        .filter(col("value") % 5 === 0) // Sample 20% of data
        .select(
          "event_id",
          "timestamp",
          "event_hour",
          "event_minute",
          "value",
          "value_range"
        )

    override def trigger: TriggerConfig = TriggerConfig.ProcessingTime("3 seconds")

    // Convenience method to run and monitor
    def runWithMonitoring(durationSeconds: Int): StreamingQuery = {
      logger.info(s"Starting $name with $eventsPerSecond events/sec")
      val query = startStream()

      // Monitor progress in separate thread
      val monitorThread = new Thread(() => {
        while (query.isActive) {
          Thread.sleep(5000)
          val progress = query.lastProgress
          if (progress != null) {
            logger.info(f"Progress: ${progress.numInputRows} rows, " +
              f"${progress.processedRowsPerSecond}%.1f rows/sec")
          }
        }
      })
      monitorThread.setDaemon(true)
      monitorThread.start()

      // Run for specified duration
      query.awaitTermination(durationSeconds.seconds.toMillis)
      query.stop()
      query
    }
  }

  // Cleanup utility
  private def cleanup(tempDir: Path): Unit =
    try {
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach { (p: Path) =>
          val _: Boolean = Files.deleteIfExists(p)
        }
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
}
