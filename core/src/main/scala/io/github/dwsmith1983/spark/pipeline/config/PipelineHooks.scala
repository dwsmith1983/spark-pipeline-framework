package io.github.dwsmith1983.spark.pipeline.config

import org.apache.logging.log4j.{LogManager, Logger}

/**
 * Lifecycle hooks for pipeline execution.
 *
 * Implement this trait to add custom behavior at various points during
 * pipeline execution, such as logging, metrics collection, alerting,
 * or custom error handling.
 *
 * All methods have default no-op implementations, so you only need to
 * override the hooks you're interested in.
 *
 * == Example ==
 *
 * {{{
 * val metricsHooks = new PipelineHooks {
 *   override def beforePipeline(config: PipelineConfig): Unit =
 *     println(s"Starting pipeline: $${config.pipelineName}")
 *
 *   override def afterComponent(
 *     config: ComponentConfig,
 *     index: Int,
 *     total: Int,
 *     durationMs: Long
 *   ): Unit =
 *     metrics.recordComponentDuration(config.instanceName, durationMs)
 *
 *   override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit =
 *     result match {
 *       case PipelineResult.Success(duration, _) =>
 *         println(s"Pipeline completed in $${duration}ms")
 *       case PipelineResult.Failure(error, _, _) =>
 *         alerting.sendAlert(s"Pipeline failed: $${error.getMessage}")
 *     }
 * }
 *
 * SimplePipelineRunner.run(config, metricsHooks)
 * }}}
 */
trait PipelineHooks {

  /**
   * Called before pipeline execution begins.
   *
   * @param config The pipeline configuration
   */
  def beforePipeline(config: PipelineConfig): Unit = {
    val _ = config // suppress unused warning - meant to be overridden
  }

  /**
   * Called after pipeline execution completes (success or failure).
   *
   * @param config The pipeline configuration
   * @param result The pipeline execution result
   */
  def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    val _ = (config, result) // suppress unused warning - meant to be overridden
  }

  /**
   * Called before each component is instantiated and run.
   *
   * @param config The component configuration
   * @param index Zero-based index of the component in the pipeline
   * @param total Total number of components in the pipeline
   */
  def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit = {
    val _ = (config, index, total) // suppress unused warning - meant to be overridden
  }

  /**
   * Called after each component completes successfully.
   *
   * @param config The component configuration
   * @param index Zero-based index of the component in the pipeline
   * @param total Total number of components in the pipeline
   * @param durationMs Execution time in milliseconds
   */
  def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    val _ = (config, index, total, durationMs) // suppress unused warning - meant to be overridden
  }

  /**
   * Called when a component fails during execution.
   *
   * This is called before the exception is propagated. The pipeline will
   * stop execution after this hook returns.
   *
   * @param config The component configuration that failed
   * @param index Zero-based index of the failed component
   * @param error The exception that was thrown
   */
  def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {
    val _ = (config, index, error) // suppress unused warning - meant to be overridden
  }

  /**
   * Called before a retry attempt for a failed component.
   *
   * This hook is called after a component fails but before the retry delay.
   * It provides visibility into retry behavior for logging and metrics.
   *
   * @param config The component configuration being retried
   * @param attempt Current retry attempt number (1-indexed, so first retry is attempt 1)
   * @param maxAttempts Maximum number of retry attempts configured
   * @param delayMs Delay in milliseconds before the retry will be executed
   * @param error The exception that triggered the retry
   */
  def onRetryAttempt(
    config: ComponentConfig,
    attempt: Int,
    maxAttempts: Int,
    delayMs: Long,
    error: Throwable
  ): Unit = {
    val _ = (config, attempt, maxAttempts, delayMs, error) // suppress unused warning - meant to be overridden
  }

  /**
   * Called when a circuit breaker changes state.
   *
   * This hook provides visibility into circuit breaker state transitions
   * for monitoring and alerting purposes.
   *
   * @param componentName Name of the component whose circuit breaker changed
   * @param oldState Previous circuit state
   * @param newState New circuit state
   */
  def onCircuitBreakerStateChange(
    componentName: String,
    oldState: CircuitState,
    newState: CircuitState
  ): Unit = {
    val _ = (componentName, oldState, newState) // suppress unused warning - meant to be overridden
  }
}

object PipelineHooks {

  private val logger: Logger = LogManager.getLogger(classOf[PipelineHooks])

  /** A no-op implementation that does nothing at each hook point. */
  val NoOp: PipelineHooks = new PipelineHooks {}

  /**
   * Combines multiple hooks into a single hook that calls each in order.
   *
   * Exceptions thrown by individual hooks are caught and logged,
   * allowing subsequent hooks to still be called. This ensures that a
   * failing logging hook won't prevent metrics collection, for example.
   *
   * @param hooks The hooks to combine
   * @return A composite hook that delegates to all provided hooks
   */
  def compose(hooks: PipelineHooks*): PipelineHooks = new PipelineHooks {

    private def safeCall(hookName: String)(f: PipelineHooks => Unit): Unit =
      hooks.foreach { hook =>
        try {
          f(hook)
        } catch {
          case e: Exception =>
            logger.warn(
              "Hook {}.{} failed: {}",
              hook.getClass.getName: Any,
              hookName: Any,
              e.getMessage: Any
            )
        }
      }

    override def beforePipeline(config: PipelineConfig): Unit =
      safeCall("beforePipeline")(_.beforePipeline(config))

    override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit =
      safeCall("afterPipeline")(_.afterPipeline(config, result))

    override def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit =
      safeCall("beforeComponent")(_.beforeComponent(config, index, total))

    override def afterComponent(
      config: ComponentConfig,
      index: Int,
      total: Int,
      durationMs: Long
    ): Unit =
      safeCall("afterComponent")(_.afterComponent(config, index, total, durationMs))

    override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit =
      safeCall("onComponentFailure")(_.onComponentFailure(config, index, error))

    override def onRetryAttempt(
      config: ComponentConfig,
      attempt: Int,
      maxAttempts: Int,
      delayMs: Long,
      error: Throwable
    ): Unit =
      safeCall("onRetryAttempt")(_.onRetryAttempt(config, attempt, maxAttempts, delayMs, error))

    override def onCircuitBreakerStateChange(
      componentName: String,
      oldState: CircuitState,
      newState: CircuitState
    ): Unit =
      safeCall("onCircuitBreakerStateChange")(_.onCircuitBreakerStateChange(componentName, oldState, newState))
  }
}
