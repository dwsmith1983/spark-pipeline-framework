package io.github.dwsmith1983.spark.pipeline

/**
 * Core configuration package for spark-pipeline-framework.
 *
 * This package provides the foundational traits and utilities for building
 * configuration-driven Spark pipelines:
 *
 * - [[config.ConfigurableInstance]] - Trait for companion objects that create instances from config
 * - [[config.PipelineComponent]] - Base trait for pipeline components with a `run()` method
 * - [[config.PipelineConfig]] - Configuration model for the pipeline definition
 * - [[config.ComponentConfig]] - Configuration model for individual components
 * - [[config.SparkConfig]] - Configuration model for Spark session settings
 * - [[config.ComponentInstantiator]] - Utility for reflection-based component instantiation
 * - [[config.PipelineHooks]] - Lifecycle hooks for pipeline execution monitoring
 * - [[config.PipelineResult]] - Result type for pipeline execution (Success/Failure)
 *
 * Example usage in user code:
 * {{{
 * import io.github.dwsmith1983.spark.pipeline.config._
 * import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow
 * import pureconfig._
 * import pureconfig.generic.auto._
 *
 * object MyComponent extends ConfigurableInstance {
 *   case class Config(inputTable: String, outputPath: String)
 *
 *   override def createFromConfig(conf: com.typesafe.config.Config): MyComponent = {
 *     new MyComponent(ConfigSource.fromConfig(conf).loadOrThrow[Config])
 *   }
 * }
 *
 * class MyComponent(conf: MyComponent.Config) extends DataFlow {
 *   override def run(): Unit = {
 *     spark.table(conf.inputTable).write.parquet(conf.outputPath)
 *   }
 * }
 * }}}
 */
package object config
