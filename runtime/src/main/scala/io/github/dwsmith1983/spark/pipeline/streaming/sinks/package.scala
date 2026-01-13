package io.github.dwsmith1983.spark.pipeline.streaming

/**
 * Streaming sink implementations for Spark Structured Streaming.
 *
 * This package provides ready-to-use streaming sinks that implement
 * the [[StreamingSink]] trait. Each sink can be configured via HOCON
 * and instantiated by the pipeline runner.
 *
 * == Available Sinks ==
 *
 * - [[sinks.KafkaStreamSink]]: Write to Kafka with exactly-once semantics
 * - [[sinks.CloudStorageStreamSink]]: Write to S3, GCS, or ADLS
 * - [[sinks.ConsoleStreamSink]]: Debug output to console
 * - [[sinks.ForeachStreamSink]]: Custom processing via user-provided classes
 * - [[sinks.DeltaLakeStreamSink]]: Write to Delta Lake (append, merge, overwrite)
 * - [[sinks.IcebergStreamSink]]: Write to Apache Iceberg (append, upsert, overwrite)
 *
 * == Usage ==
 *
 * Sinks are typically used within a [[StreamingPipeline]]:
 *
 * {{{
 * class MyPipeline(source: StreamingSource, sink: StreamingSink)
 *     extends StreamingPipeline {
 *   // Pipeline orchestrates source -> transform -> sink
 * }
 * }}}
 *
 * @see [[StreamingSink]] for the base trait
 * @see [[StreamingPipeline]] for orchestrating streaming workflows
 */
package object sinks
