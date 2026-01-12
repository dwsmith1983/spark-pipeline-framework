package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import scala.language.existentials
import scala.util.{Failure, Success, Try}

/**
 * Validates pipeline configuration without instantiating components.
 *
 * This provides faster validation than [[DryRunResult dry-run]] by checking
 * configuration structure and class availability without calling
 * `createFromConfig`. Use this for pre-flight checks in CI/CD pipelines.
 *
 * == Validation Phases ==
 *
 *   1. '''ConfigSyntax''': HOCON syntax correctness
 *   1. '''RequiredFields''': pipelineName and pipelineComponents present
 *   1. '''TypeResolution''': Classes and companion objects exist
 *   1. '''ComponentConfig''': Companion objects extend ConfigurableInstance
 *   1. '''ResourceValidation''': (Optional) Referenced paths/tables exist
 *
 * == Example ==
 *
 * {{{
 * val result = ConfigValidator.validate(config)
 * if (result.isInvalid) {
 *   result.errors.foreach(e => println(e.fullMessage))
 * }
 * }}}
 */
object ConfigValidator {

  /**
   * Validates a configuration.
   *
   * @param config The HOCON configuration to validate
   * @return ValidationResult with all errors and warnings
   */
  def validate(config: Config): ValidationResult =
    validate(config, ValidationOptions.Default)

  /**
   * Validates a configuration with options.
   *
   * @param config  The HOCON configuration to validate
   * @param options Validation options (e.g., resource checking)
   * @return ValidationResult with all errors and warnings
   */
  def validate(config: Config, options: ValidationOptions): ValidationResult = {
    val errors   = List.newBuilder[ValidationError]
    val warnings = List.newBuilder[ValidationWarning]

    // Phase 2: Required fields
    val pipelineConfigResult = validateRequiredFields(config, errors)

    pipelineConfigResult match {
      case None =>
        ValidationResult.Invalid(errors.result(), warnings.result())

      case Some(pipelineConfig) =>
        pipelineConfig.pipelineComponents.zipWithIndex.foreach {
          case (componentConfig, idx) =>
            validateComponent(componentConfig, idx, errors)
        }

        if (options.validateResources) {
          validateResources(pipelineConfig, options, errors, warnings)
        }

        val errorList = errors.result()
        if (errorList.isEmpty) {
          ValidationResult.Valid(
            pipelineConfig.pipelineName,
            pipelineConfig.pipelineComponents.size,
            warnings.result()
          )
        } else {
          ValidationResult.Invalid(errorList, warnings.result())
        }
    }
  }

  /**
   * Validates a configuration from a HOCON string.
   *
   * @param hoconString The HOCON configuration string
   * @return ValidationResult with all errors and warnings
   */
  def validateFromString(hoconString: String): ValidationResult =
    validateFromString(hoconString, ValidationOptions.Default)

  /**
   * Validates a configuration from a HOCON string with options.
   *
   * @param hoconString The HOCON configuration string
   * @param options     Validation options
   * @return ValidationResult with all errors and warnings
   */
  def validateFromString(hoconString: String, options: ValidationOptions): ValidationResult =
    Try(ConfigFactory.parseString(hoconString)) match {
      case Success(config) => validate(config, options)
      case Failure(e: ConfigException) =>
        ValidationResult.invalid(
          ValidationError(
            ValidationPhase.ConfigSyntax,
            ErrorLocation.Pipeline,
            s"Failed to parse HOCON: ${e.getMessage}",
            Some(e)
          )
        )
      case Failure(e) =>
        ValidationResult.invalid(
          ValidationError(
            ValidationPhase.ConfigSyntax,
            ErrorLocation.Pipeline,
            s"Unexpected error parsing configuration: ${e.getMessage}",
            Some(e)
          )
        )
    }

  /**
   * Validates a configuration from a file path.
   *
   * @param configPath Path to the HOCON configuration file
   * @return ValidationResult with all errors and warnings
   */
  def validateFromFile(configPath: String): ValidationResult =
    validateFromFile(configPath, ValidationOptions.Default)

  /**
   * Validates a configuration from a file path with options.
   *
   * @param configPath Path to the HOCON configuration file
   * @param options    Validation options
   * @return ValidationResult with all errors and warnings
   */
  def validateFromFile(configPath: String, options: ValidationOptions): ValidationResult = {
    val file = new java.io.File(configPath)
    if (!file.exists()) {
      ValidationResult.invalid(
        ValidationError(
          ValidationPhase.ConfigSyntax,
          ErrorLocation.Pipeline,
          s"Configuration file not found: $configPath"
        )
      )
    } else {
      Try(ConfigFactory.parseFile(file)) match {
        case Success(config) => validate(config, options)
        case Failure(e: ConfigException) =>
          ValidationResult.invalid(
            ValidationError(
              ValidationPhase.ConfigSyntax,
              ErrorLocation.Pipeline,
              s"Failed to parse configuration file: ${e.getMessage}",
              Some(e)
            )
          )
        case Failure(e) =>
          ValidationResult.invalid(
            ValidationError(
              ValidationPhase.ConfigSyntax,
              ErrorLocation.Pipeline,
              s"Unexpected error reading configuration file: ${e.getMessage}",
              Some(e)
            )
          )
      }
    }
  }

  private def validateRequiredFields(
    config: Config,
    errors: scala.collection.mutable.Builder[ValidationError, List[ValidationError]]
  ): Option[PipelineConfig] =
    if (!config.hasPath("pipeline")) {
      errors += ValidationError(
        ValidationPhase.RequiredFields,
        ErrorLocation.Pipeline,
        "Missing required 'pipeline' block in configuration"
      )
      None
    } else {
      Try(ConfigSource.fromConfig(config.getConfig("pipeline")).loadOrThrow[PipelineConfig]) match {
        case Success(pipelineConfig) =>
          if (pipelineConfig.pipelineComponents.isEmpty) {
            errors += ValidationError(
              ValidationPhase.RequiredFields,
              ErrorLocation.Pipeline,
              "Pipeline must have at least one component in 'pipeline-components'"
            )
          }
          Some(pipelineConfig)

        case Failure(e: pureconfig.error.ConfigReaderException[_]) =>
          errors += ValidationError(
            ValidationPhase.RequiredFields,
            ErrorLocation.Pipeline,
            s"Invalid pipeline configuration: ${e.failures.toList.map(_.description).mkString("; ")}",
            Some(e)
          )
          None

        case Failure(e) =>
          errors += ValidationError(
            ValidationPhase.RequiredFields,
            ErrorLocation.Pipeline,
            s"Failed to parse pipeline configuration: ${e.getMessage}",
            Some(e)
          )
          None
      }
    }

  private def validateComponent(
    componentConfig: ComponentConfig,
    index: Int,
    errors: scala.collection.mutable.Builder[ValidationError, List[ValidationError]]
  ): Unit = {
    val location = ErrorLocation.Component(
      index,
      componentConfig.instanceType,
      componentConfig.instanceName
    )
    val className = componentConfig.instanceType

    // Phase 3: Type resolution - verify class exists
    val classLoadResult = Try(Class.forName(className))

    classLoadResult match {
      case Failure(_: ClassNotFoundException) =>
        errors += ValidationError(
          ValidationPhase.TypeResolution,
          location,
          s"Class not found: $className. Ensure the class is on the classpath."
        )

      case Failure(e) =>
        errors += ValidationError(
          ValidationPhase.TypeResolution,
          location,
          s"Error loading class $className: ${e.getMessage}",
          Some(e)
        )

      case Success(_) =>
        // Class exists, now verify companion object
        validateCompanionObject(className, location, errors)
    }
  }

  private def validateCompanionObject(
    className: String,
    location: ErrorLocation,
    errors: scala.collection.mutable.Builder[ValidationError, List[ValidationError]]
  ): Unit = {
    val companionClassResult = Try(Class.forName(className + "$"))

    companionClassResult match {
      case Failure(_: ClassNotFoundException) =>
        errors += ValidationError(
          ValidationPhase.TypeResolution,
          location,
          s"Companion object not found for $className. " +
            "Ensure it has a companion object extending ConfigurableInstance."
        )

      case Failure(e) =>
        errors += ValidationError(
          ValidationPhase.TypeResolution,
          location,
          s"Error loading companion object for $className: ${e.getMessage}",
          Some(e)
        )

      case Success(companionClass) =>
        val moduleResult = Try(companionClass.getField("MODULE$").get(null))
        moduleResult match {
          case Failure(e) =>
            errors += ValidationError(
              ValidationPhase.TypeResolution,
              location,
              s"Could not access companion object for $className: ${e.getMessage}",
              Some(e)
            )

          case Success(companion) =>
            // Phase 4: Verify companion extends ConfigurableInstance
            if (!companion.isInstanceOf[ConfigurableInstance]) {
              errors += ValidationError(
                ValidationPhase.ComponentConfig,
                location,
                s"Companion object of $className does not extend ConfigurableInstance"
              )
            }
        }
    }
  }

  private def validateResources(
    pipelineConfig: PipelineConfig,
    options: ValidationOptions,
    errors: scala.collection.mutable.Builder[ValidationError, List[ValidationError]],
    warnings: scala.collection.mutable.Builder[ValidationWarning, List[ValidationWarning]]
  ): Unit = {
    val _ = (pipelineConfig, options, errors) // suppress unused warnings
    warnings += ValidationWarning(
      ErrorLocation.Pipeline,
      "Resource validation is limited. Use dryRun for complete validation including " +
        "component instantiation and config parsing."
    )
  }
}
