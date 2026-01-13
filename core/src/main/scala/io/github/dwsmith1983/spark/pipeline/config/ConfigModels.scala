package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.Config
import io.github.dwsmith1983.spark.pipeline.checkpoint.CheckpointConfig

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
 *   fail-fast = true  # Optional, defaults to true
 *   schema-validation {  # Optional
 *     enabled = true
 *     strict = false
 *     fail-on-warning = false
 *   }
 *
 *   # Optional checkpoint configuration for resume support
 *   checkpoint {
 *     enabled = true
 *     location = "/tmp/spark-checkpoints"
 *     auto-resume = false
 *     cleanup-on-success = true
 *   }
 *
 *   retry-policy {    # Optional, global default retry policy
 *     max-retries = 3
 *     initial-delay-ms = 1000
 *   }
 *   circuit-breaker { # Optional, enables circuit breaker for all components
 *     failure-threshold = 5
 *     reset-timeout-ms = 60000
 *   }
 *   pipeline-components = [...]
 * }
 * }}}
 *
 * @param pipelineName Human-readable name for the pipeline
 * @param pipelineComponents List of components to execute in order
 * @param failFast If true (default), stop on first component failure.
 *                 If false, continue executing remaining components and report all failures.
 * @param schemaValidation Optional schema validation configuration. When enabled,
 *                         the runner validates schema contracts between adjacent components.
 * @param checkpoint Optional checkpoint configuration for resume functionality
 * @param retryPolicy Optional default retry policy for all components.
 *                    Components can override this with their own retry-policy.
 * @param circuitBreaker Optional circuit breaker configuration.
 *                       When set, enables circuit breaker protection for components.
 */
case class PipelineConfig(
  pipelineName: String,
  pipelineComponents: List[ComponentConfig],
  failFast: Boolean = true,
  schemaValidation: Option[SchemaValidationConfig] = None,
  checkpoint: Option[CheckpointConfig] = None,
  retryPolicy: Option[RetryPolicy] = None,
  circuitBreaker: Option[CircuitBreakerConfig] = None)

/**
 * Configuration for a single pipeline component.
 *
 * Example HOCON:
 * {{{
 * {
 *   instance-type = "com.yourcompany.MyComponent"
 *   instance-name = "MyComponent(prod)"
 *   instance-config {
 *     input-table = "raw_data"
 *     output-path = "/data/processed"
 *   }
 *   retry-policy {  # Optional, overrides pipeline-level retry-policy
 *     max-retries = 5
 *     initial-delay-ms = 500
 *   }
 * }
 * }}}
 *
 * @param instanceType Fully qualified class name of the component
 * @param instanceName Human-readable name for this instance
 * @param instanceConfig Raw config to pass to the component's factory method
 * @param retryPolicy Optional retry policy for this component.
 *                    Overrides the pipeline-level retry-policy if set.
 */
case class ComponentConfig(
  instanceType: String,
  instanceName: String,
  instanceConfig: Config,
  retryPolicy: Option[RetryPolicy] = None)

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
