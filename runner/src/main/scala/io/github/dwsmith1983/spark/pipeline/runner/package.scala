package io.github.dwsmith1983.spark.pipeline

/**
 * Runner package for spark-pipeline-framework.
 *
 * This package provides the entry point for executing pipelines:
 *
 * - [[runner.PipelineRunner]] - Trait defining the pipeline execution interface
 * - [[runner.SimplePipelineRunner]] - Main class for spark-submit (sequential execution)
 *
 * == Lifecycle Hooks ==
 *
 * Use `PipelineHooks` from the config package to monitor
 * or customize pipeline execution:
 *
 * {{{
 * import io.github.dwsmith1983.spark.pipeline.config._
 *
 * val hooks = new PipelineHooks {
 *   override def afterComponent(config: ComponentConfig, index: Int, total: Int, durationMs: Long): Unit =
 *     println(s"Component $${config.instanceName} completed in $${durationMs}ms")
 * }
 *
 * SimplePipelineRunner.run(config, hooks)
 * }}}
 *
 * == Deployment ==
 *
 * The runner JAR should be used as the main artifact for spark-submit,
 * with user pipeline JARs added via --jars:
 *
 * {{{
 * spark-submit \
 *   --class io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner \
 *   --jars /path/to/user-pipeline.jar \
 *   /path/to/spark-pipeline-runner-spark3_2.12.jar \
 *   -Dconfig.file=/path/to/pipeline.conf
 * }}}
 *
 * == Configuration Loading ==
 *
 * Configuration is loaded via Typesafe Config in this order:
 * 1. System properties (-Dconfig.file, -Dconfig.resource, -Dconfig.url)
 * 2. application.conf in classpath
 * 3. reference.conf in classpath
 */
package object runner
