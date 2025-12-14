package io.github.dwsmith1983.spark.pipeline

/**
 * Runner package for spark-pipeline-framework.
 *
 * This package provides the entry point for executing pipelines:
 *
 * - [[runner.SimplePipelineRunner]] - Main class for spark-submit
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
