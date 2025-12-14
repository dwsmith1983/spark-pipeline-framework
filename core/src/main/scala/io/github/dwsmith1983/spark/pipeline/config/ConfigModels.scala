package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.Config

/**
 * Configuration models for the pipeline framework.
 * These case classes are used to parse HOCON configuration files.
 */

/**
 * Root pipeline configuration.
 *
 * Example HOCON:
 * {{{
 * pipeline {
 *   pipeline-name = "My Data Pipeline"
 *   pipeline-components = [...]
 * }
 * }}}
 *
 * @param pipelineName Human-readable name for the pipeline
 * @param pipelineComponents List of components to execute in order
 */
case class PipelineConfig(
  pipelineName: String,
  pipelineComponents: List[ComponentConfig])

/**
 * Configuration for a single pipeline component.
 *
 * Example HOCON:
 * {{{
 * {
 *   instance-type = "com.example.MyComponent"
 *   instance-name = "MyComponent(prod)"
 *   instance-config {
 *     input-table = "raw_data"
 *     output-path = "/data/processed"
 *   }
 * }
 * }}}
 *
 * @param instanceType Fully qualified class name of the component
 * @param instanceName Human-readable name for this instance
 * @param instanceConfig Raw config to pass to the component's factory method
 */
case class ComponentConfig(
  instanceType: String,
  instanceName: String,
  instanceConfig: Config)

/**
 * Spark session configuration.
 *
 * Example HOCON:
 * {{{
 * spark {
 *   master = "yarn"
 *   app-name = "MyPipeline"
 *   config {
 *     "spark.executor.memory" = "4g"
 *     "spark.executor.cores" = "2"
 *     "spark.dynamicAllocation.enabled" = "true"
 *   }
 * }
 * }}}
 *
 * @param master Spark master URL (local, yarn, spark://host:port)
 * @param appName Application name shown in Spark UI
 * @param config Additional Spark configuration key-value pairs
 */
case class SparkConfig(
  master: Option[String] = None,
  appName: Option[String] = None,
  config: Map[String, String] = Map.empty)
