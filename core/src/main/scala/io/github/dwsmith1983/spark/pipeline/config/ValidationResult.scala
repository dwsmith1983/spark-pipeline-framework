package io.github.dwsmith1983.spark.pipeline.config

/**
 * Result of pipeline configuration validation.
 *
 * Validation checks the configuration structure and class availability without
 * instantiating components. This is faster than [[DryRunResult dry-run]] and has
 * no side effects from component initialization.
 *
 * Use `validate` for quick pre-flight checks, and `dryRun` when you need to
 * verify that components can be fully instantiated.
 *
 * == Example ==
 *
 * {{{
 * val result = SimplePipelineRunner.validate(config)
 * result match {
 *   case ValidationResult.Valid(pipelineName, componentCount, warns) =>
 *     warns.foreach(w => println("Warning: " + w.message))
 *     println("Pipeline '" + pipelineName + "' is valid with " + componentCount + " components")
 *   case ValidationResult.Invalid(errs, warns) =>
 *     errs.foreach(e => println("Error: " + e.message))
 *     sys.exit(1)
 * }
 * }}}
 */
sealed trait ValidationResult {

  /** Whether the validation passed. */
  def isValid: Boolean

  /** Whether the validation failed. */
  def isInvalid: Boolean = !isValid

  /** All validation errors encountered. Empty for valid results. */
  def errors: List[ValidationError]

  /** Non-fatal warnings encountered during validation. */
  def warnings: List[ValidationWarning]

  /**
   * Combine this result with another, accumulating errors and warnings.
   *
   * If either result is invalid, the combined result is invalid.
   * Warnings are accumulated from both results.
   */
  def ++(other: ValidationResult): ValidationResult
}

object ValidationResult {

  /**
   * Indicates the pipeline configuration is valid.
   *
   * All validation phases passed:
   * - Configuration syntax is correct
   * - Required fields are present
   * - Component classes exist and are accessible
   * - Component companion objects extend [[ConfigurableInstance]]
   *
   * @param pipelineName   The name of the validated pipeline
   * @param componentCount Number of components in the pipeline
   * @param warnings       Non-fatal warnings encountered
   */
  final case class Valid(
    pipelineName: String,
    componentCount: Int,
    warnings: List[ValidationWarning] = List.empty)
    extends ValidationResult {

    override def isValid: Boolean              = true
    override def errors: List[ValidationError] = List.empty

    override def ++(other: ValidationResult): ValidationResult = other match {
      case Valid(_, _, otherWarnings) =>
        copy(warnings = warnings ++ otherWarnings)
      case Invalid(otherErrors, otherWarnings) =>
        Invalid(otherErrors, warnings ++ otherWarnings)
    }
  }

  /**
   * Indicates the pipeline configuration is invalid.
   *
   * @param errors   List of validation errors encountered
   * @param warnings Non-fatal warnings encountered
   */
  final case class Invalid(
    errors: List[ValidationError],
    warnings: List[ValidationWarning] = List.empty)
    extends ValidationResult {

    override def isValid: Boolean = false

    override def ++(other: ValidationResult): ValidationResult = other match {
      case Valid(_, _, otherWarnings) =>
        copy(warnings = warnings ++ otherWarnings)
      case Invalid(otherErrors, otherWarnings) =>
        Invalid(errors ++ otherErrors, warnings ++ otherWarnings)
    }
  }

  /** Create an invalid result from a single error. */
  def invalid(error: ValidationError): Invalid = Invalid(List(error))

  /** Create a valid result with no warnings. */
  def valid(pipelineName: String, componentCount: Int): Valid =
    Valid(pipelineName, componentCount)
}

/** Phase of validation where an error occurred. */
sealed trait ValidationPhase {
  def name: String
}

object ValidationPhase {

  /** HOCON syntax validation. */
  case object ConfigSyntax extends ValidationPhase {
    override def name: String = "config-syntax"
  }

  /** Required fields validation (pipelineName, pipelineComponents). */
  case object RequiredFields extends ValidationPhase {
    override def name: String = "required-fields"
  }

  /** Class and companion object resolution. */
  case object TypeResolution extends ValidationPhase {
    override def name: String = "type-resolution"
  }

  /** Component configuration structure validation. */
  case object ComponentConfig extends ValidationPhase {
    override def name: String = "component-config"
  }

  /** Optional resource existence validation (paths, tables). */
  case object ResourceValidation extends ValidationPhase {
    override def name: String = "resource-validation"
  }
}

/** Location in the configuration where an error occurred. */
sealed trait ErrorLocation {
  def description: String
}

object ErrorLocation {

  /** Error at the pipeline level (not specific to a component). */
  case object Pipeline extends ErrorLocation {
    override def description: String = "pipeline configuration"
  }

  /**
   * Error for a specific component.
   *
   * @param index        Zero-based index of the component in the pipeline
   * @param instanceType Fully qualified class name
   * @param instanceName Human-readable instance name
   */
  final case class Component(index: Int, instanceType: String, instanceName: String)
    extends ErrorLocation {
    override def description: String = s"component[$index] '$instanceName' ($instanceType)"
  }
}

/**
 * A validation error with phase and location context.
 *
 * @param phase    The validation phase where the error occurred
 * @param location Where in the configuration the error occurred
 * @param message  Human-readable description of the error
 * @param cause    The underlying exception, if any
 */
final case class ValidationError(
  phase: ValidationPhase,
  location: ErrorLocation,
  message: String,
  cause: Option[Throwable] = None) {

  /** Formatted error message including phase and location. */
  def fullMessage: String =
    s"[${phase.name}] ${location.description}: $message"
}

/**
 * A non-fatal validation warning.
 *
 * @param location Where in the configuration the warning applies
 * @param message  Human-readable description of the warning
 */
final case class ValidationWarning(location: ErrorLocation, message: String) {

  /** Formatted warning message including location. */
  def fullMessage: String = s"${location.description}: $message"
}

/**
 * Options for controlling validation behavior.
 *
 * @param validateResources Whether to validate that referenced paths and tables exist.
 *                          Defaults to false for fast validation. Note: Resource validation
 *                          is currently limited and serves as a placeholder for future
 *                          enhancements.
 */
final case class ValidationOptions(validateResources: Boolean = false)

object ValidationOptions {

  /** Default options: fast validation without resource checks. */
  val Default: ValidationOptions = ValidationOptions()
}
