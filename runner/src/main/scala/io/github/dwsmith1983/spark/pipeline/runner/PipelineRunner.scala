package io.github.dwsmith1983.spark.pipeline.runner

import com.typesafe.config.Config
import io.github.dwsmith1983.spark.pipeline.config.PipelineHooks

/**
 * Trait defining the interface for pipeline execution.
 *
 * Implementations of this trait are responsible for:
 * - Loading and parsing pipeline configuration
 * - Configuring the SparkSession
 * - Instantiating and executing pipeline components
 * - Invoking lifecycle hooks at appropriate points
 *
 * == Standard Implementation ==
 *
 * The framework provides [[SimplePipelineRunner]] as the standard implementation,
 * which executes components sequentially in the order they appear in the configuration.
 *
 * == Custom Implementations ==
 *
 * You can create custom runners for specialized behavior:
 * - Parallel execution of independent components
 * - Dry-run mode that validates without executing
 * - Conditional execution based on external state
 *
 * == Example ==
 *
 * {{{
 * // Using the standard runner with custom hooks
 * val hooks = new PipelineHooks {
 *   override def afterComponent(config: ComponentConfig, index: Int, total: Int, durationMs: Long): Unit =
 *     println(s"Component $${config.instanceName} completed in $${durationMs}ms")
 * }
 *
 * SimplePipelineRunner.run(config, hooks)
 * }}}
 */
trait PipelineRunner {

  /**
   * Runs the pipeline defined in the given configuration.
   *
   * This is a convenience method that uses `PipelineHooks.NoOp`.
   *
   * May throw `ComponentInstantiationException` if a component cannot be instantiated,
   * or `ConfigReaderException` if the configuration is invalid.
   *
   * @param config The loaded HOCON configuration containing `pipeline` and optionally `spark` blocks
   */
  def run(config: Config): Unit = run(config, PipelineHooks.NoOp)

  /**
   * Runs the pipeline defined in the given configuration with lifecycle hooks.
   *
   * May throw `ComponentInstantiationException` if a component cannot be instantiated,
   * or `ConfigReaderException` if the configuration is invalid.
   *
   * @param config The loaded HOCON configuration containing `pipeline` and optionally `spark` blocks
   * @param hooks Lifecycle hooks to invoke during execution
   */
  def run(config: Config, hooks: PipelineHooks): Unit
}
