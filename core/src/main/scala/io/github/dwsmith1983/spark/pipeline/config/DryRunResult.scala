package io.github.dwsmith1983.spark.pipeline.config

/**
 * Result of a pipeline dry-run validation.
 *
 * A dry-run validates the pipeline configuration and component instantiation
 * without actually executing the components. This is useful for CI/CD pipelines
 * to validate configurations before deployment.
 *
 * == Example ==
 *
 * {{{
 * val result = SimplePipelineRunner.dryRun(config)
 * result match {
 *   case DryRunResult.Valid(name, count, _) =>
 *     println(s"Pipeline '$$name' is valid with $$count components")
 *   case DryRunResult.Invalid(errors) =>
 *     errors.foreach(e => println(s"Error: $$e"))
 *     sys.exit(1)
 * }
 * }}}
 */
sealed trait DryRunResult {

  /** Whether the dry-run validation passed. */
  def isValid: Boolean

  /** Whether the dry-run validation failed. */
  def isInvalid: Boolean = !isValid
}

object DryRunResult {

  /**
   * Indicates the pipeline configuration is valid.
   *
   * All components were successfully instantiated, meaning:
   * - The configuration parses correctly
   * - All component classes exist and are accessible
   * - All component companion objects have valid `createFromConfig` methods
   * - All component configurations are valid
   *
   * @param pipelineName The name of the validated pipeline
   * @param componentCount Number of components in the pipeline
   * @param components The validated component configurations
   */
  final case class Valid(
    pipelineName: String,
    componentCount: Int,
    components: List[ComponentConfig])
    extends DryRunResult {
    override def isValid: Boolean = true
  }

  /**
   * Indicates the pipeline configuration is invalid.
   *
   * @param errors List of validation errors encountered
   */
  final case class Invalid(errors: List[DryRunError]) extends DryRunResult {
    override def isValid: Boolean = false
  }
}

/** Error encountered during dry-run validation. */
sealed trait DryRunError {

  /** Human-readable description of the error. */
  def message: String

  /** The underlying cause, if any. */
  def cause: Option[Throwable]
}

object DryRunError {

  /**
   * Error parsing the pipeline or Spark configuration.
   *
   * @param message Description of the parse error
   * @param underlying The underlying exception
   */
  final case class ConfigParseError(message: String, underlying: Throwable) extends DryRunError {
    override def cause: Option[Throwable] = Some(underlying)
  }

  /**
   * Error instantiating a pipeline component.
   *
   * This typically means the component class doesn't exist, doesn't have
   * a companion object with `createFromConfig`, or the component's
   * configuration is invalid.
   *
   * @param component The component configuration that failed
   * @param underlying The underlying exception
   */
  final case class ComponentInstantiationError(
    component: ComponentConfig,
    underlying: Throwable)
    extends DryRunError {

    override def message: String =
      s"Failed to instantiate component '${component.instanceName}' (${component.instanceType}): ${underlying.getMessage}"
    override def cause: Option[Throwable] = Some(underlying)
  }
}
