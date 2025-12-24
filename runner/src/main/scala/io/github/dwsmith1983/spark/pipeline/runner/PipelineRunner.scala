package io.github.dwsmith1983.spark.pipeline.runner

import com.typesafe.config.Config
import io.github.dwsmith1983.spark.pipeline.config.{DryRunResult, PipelineHooks}

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
   * or `ConfigurationException` if the configuration is invalid.
   *
   * @param config The loaded HOCON configuration containing `pipeline` and optionally `spark` blocks
   */
  def run(config: Config): Unit = run(config, PipelineHooks.NoOp)

  /**
   * Runs the pipeline defined in the given configuration with lifecycle hooks.
   *
   * May throw `ComponentInstantiationException` if a component cannot be instantiated,
   * or `ConfigurationException` if the configuration is invalid.
   *
   * @param config The loaded HOCON configuration containing `pipeline` and optionally `spark` blocks
   * @param hooks Lifecycle hooks to invoke during execution
   */
  def run(config: Config, hooks: PipelineHooks): Unit

  /**
   * Validates the pipeline configuration without executing components.
   *
   * This method parses the configuration and instantiates all components
   * to verify they can be created, but does not call their `run()` methods.
   * Useful for CI/CD pipelines to validate configurations before deployment.
   *
   * This is a convenience method that uses `PipelineHooks.NoOp`.
   *
   * @param config The loaded HOCON configuration containing `pipeline` and optionally `spark` blocks
   * @return Validation result indicating success or listing errors
   */
  def dryRun(config: Config): DryRunResult = dryRun(config, PipelineHooks.NoOp)

  /**
   * Validates the pipeline configuration without executing components.
   *
   * This method parses the configuration and instantiates all components
   * to verify they can be created, but does not call their `run()` methods.
   * Lifecycle hooks are invoked for `beforePipeline` and `afterPipeline` only.
   *
   * @param config The loaded HOCON configuration containing `pipeline` and optionally `spark` blocks
   * @param hooks Lifecycle hooks to invoke (only beforePipeline/afterPipeline are called)
   * @return Validation result indicating success or listing errors
   */
  def dryRun(config: Config, hooks: PipelineHooks): DryRunResult
}
