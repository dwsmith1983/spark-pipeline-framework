package io.github.dwsmith1983.spark.pipeline.config

/**
 * Schema contracts for pipeline components.
 *
 * Components can optionally declare their input and output schemas to enable
 * runtime validation between adjacent components in a pipeline. This helps
 * catch schema mismatches early with clear error messages, rather than
 * failing deep in component execution with unclear errors.
 *
 * == Overview ==
 *
 * Schema contracts are entirely optional and backward compatible:
 *
 *   - Components without contracts continue to work unchanged
 *   - Validation only occurs when both producer and consumer declare contracts
 *   - Schema validation is disabled by default in pipeline config
 *
 * == Usage ==
 *
 * {{{
 * class MyTransform(conf: MyTransform.Config) extends DataFlow with SchemaContract {
 *   import spark.implicits._
 *
 *   override def inputContract: Option[SchemaDefinition] = Some(
 *     SchemaDefinition(Seq(
 *       SchemaField("user_id", "string", nullable = false),
 *       SchemaField("amount", "double")
 *     ))
 *   )
 *
 *   override def outputContract: Option[SchemaDefinition] = Some(
 *     SchemaDefinition(Seq(
 *       SchemaField("user_id", "string", nullable = false),
 *       SchemaField("amount", "double"),
 *       SchemaField("processed_at", "timestamp")
 *     ))
 *   )
 *
 *   override def run(): Unit = {
 *     // Implementation
 *   }
 * }
 * }}}
 *
 * == Configuration ==
 *
 * Enable schema validation in pipeline config:
 *
 * {{{
 * pipeline {
 *   pipeline-name = "My Pipeline"
 *   schema-validation {
 *     enabled = true
 *     strict = false        // If true, all components must have contracts
 *     fail-on-warning = false  // If true, warnings become errors
 *   }
 *   pipeline-components = [...]
 * }
 * }}}
 */
trait SchemaContract {

  /**
   * Optional schema that this component expects from its input data.
   *
   * When declared, the pipeline runner validates that the previous component's
   * output contract (if any) is compatible with this input contract.
   *
   * @return The expected input schema, or None if not declared
   */
  def inputContract: Option[SchemaDefinition] = None

  /**
   * Optional schema that this component produces as output.
   *
   * When declared, the pipeline runner validates that this output contract
   * is compatible with the next component's input contract (if any).
   *
   * @return The produced output schema, or None if not declared
   */
  def outputContract: Option[SchemaDefinition] = None
}

/**
 * A platform-independent schema definition.
 *
 * This class provides a lightweight representation of DataFrame schemas that
 * does not depend on Spark. It can be converted to/from Spark's StructType
 * in the runtime module.
 *
 * @param fields      The fields in this schema
 * @param description Optional human-readable description of the schema
 */
final case class SchemaDefinition(
  fields: Seq[SchemaField],
  description: Option[String] = None) {

  /** Field names in this schema. */
  def fieldNames: Seq[String] = fields.map(_.name)

  /** Get a field by name, if it exists. */
  def field(name: String): Option[SchemaField] = fields.find(_.name == name)

  /** Check if this schema contains a field with the given name. */
  def hasField(name: String): Boolean = fields.exists(_.name == name)

  /** Number of fields in this schema. */
  def size: Int = fields.size
}

object SchemaDefinition {

  /** Create an empty schema definition. */
  val empty: SchemaDefinition = SchemaDefinition(Seq.empty)

  /**
   * Create a schema definition from field tuples.
   *
   * Convenience method for simple schema declarations:
   * {{{
   * SchemaDefinition.of(
   *   ("user_id", "string", false),
   *   ("amount", "double", true)
   * )
   * }}}
   */
  def of(fields: (String, String, Boolean)*): SchemaDefinition =
    SchemaDefinition(fields.map { case (name, dataType, nullable) =>
      SchemaField(name, dataType, nullable)
    })
}

/**
 * A single field in a schema definition.
 *
 * @param name     The field name
 * @param dataType The data type as a string (e.g., "string", "integer", "double",
 *                 "boolean", "timestamp", "date", "binary", "array", "map", "struct")
 * @param nullable Whether the field can contain null values (default: true)
 * @param metadata Optional key-value metadata for the field
 */
final case class SchemaField(
  name: String,
  dataType: String,
  nullable: Boolean = true,
  metadata: Map[String, String] = Map.empty) {

  /** Normalized data type (lowercase, trimmed). */
  def normalizedDataType: String = dataType.toLowerCase.trim
}

object SchemaField {

  /** Common data type constants. */
  object DataTypes {
    val String: String    = "string"
    val Integer: String   = "integer"
    val Long: String      = "long"
    val Double: String    = "double"
    val Float: String     = "float"
    val Boolean: String   = "boolean"
    val Timestamp: String = "timestamp"
    val Date: String      = "date"
    val Binary: String    = "binary"
    val Decimal: String   = "decimal"
    val Array: String     = "array"
    val Map: String       = "map"
    val Struct: String    = "struct"
  }
}

/**
 * Configuration for schema validation behavior.
 *
 * @param enabled       Whether schema validation is enabled (default: false)
 * @param strict        If true, all components must declare schemas (default: false)
 * @param failOnWarning If true, validation warnings are treated as errors (default: false)
 */
final case class SchemaValidationConfig(
  enabled: Boolean = false,
  strict: Boolean = false,
  failOnWarning: Boolean = false)

object SchemaValidationConfig {

  /** Default configuration: validation disabled. */
  val Default: SchemaValidationConfig = SchemaValidationConfig()

  /** Enabled configuration with default options. */
  val Enabled: SchemaValidationConfig = SchemaValidationConfig(enabled = true)

  /** Strict configuration: enabled with all contracts required. */
  val Strict: SchemaValidationConfig =
    SchemaValidationConfig(enabled = true, strict = true, failOnWarning = true)
}

/**
 * Result of schema validation between two components.
 */
sealed trait SchemaValidationResult {

  /** Whether the validation passed without errors. */
  def isValid: Boolean

  /** Whether the validation failed. */
  def isInvalid: Boolean = !isValid

  /** All validation errors encountered. */
  def errors: List[SchemaValidationError]

  /** Non-fatal warnings encountered. */
  def warnings: List[SchemaValidationWarning]
}

object SchemaValidationResult {

  /**
   * Schema validation passed.
   *
   * @param warnings Non-fatal warnings encountered
   */
  final case class Valid(warnings: List[SchemaValidationWarning] = List.empty)
    extends SchemaValidationResult {

    override def isValid: Boolean                         = true
    override def errors: List[SchemaValidationError] = List.empty
  }

  /**
   * Schema validation failed.
   *
   * @param errors   Validation errors that caused the failure
   * @param warnings Non-fatal warnings also encountered
   */
  final case class Invalid(
    errors: List[SchemaValidationError],
    warnings: List[SchemaValidationWarning] = List.empty)
    extends SchemaValidationResult {

    override def isValid: Boolean = false
  }

  /** Validation skipped because contracts were not declared. */
  case object Skipped extends SchemaValidationResult {
    override def isValid: Boolean                           = true
    override def errors: List[SchemaValidationError]   = List.empty
    override def warnings: List[SchemaValidationWarning] = List.empty
  }

  /** Create an invalid result from a single error. */
  def invalid(error: SchemaValidationError): Invalid = Invalid(List(error))

  /** Create a valid result with no warnings. */
  val valid: Valid = Valid()
}

/**
 * A schema validation error.
 *
 * @param errorType   The type of schema error
 * @param message     Human-readable description
 * @param fieldName   The field name involved, if applicable
 * @param expected    Expected value (type, nullability, etc.)
 * @param actual      Actual value found
 */
final case class SchemaValidationError(
  errorType: SchemaErrorType,
  message: String,
  fieldName: Option[String] = None,
  expected: Option[String] = None,
  actual: Option[String] = None) {

  /** Formatted error message with all details. */
  def fullMessage: String = {
    val details = List(
      fieldName.map(f => s"field: $f"),
      expected.map(e => s"expected: $e"),
      actual.map(a => s"actual: $a")
    ).flatten.mkString(", ")
    if (details.nonEmpty) s"$message ($details)" else message
  }
}

/** Types of schema validation errors. */
sealed trait SchemaErrorType {
  def name: String
}

object SchemaErrorType {

  /** A required field is missing from the output schema. */
  case object MissingField extends SchemaErrorType {
    override def name: String = "missing-field"
  }

  /** Field types are incompatible. */
  case object TypeMismatch extends SchemaErrorType {
    override def name: String = "type-mismatch"
  }

  /** A non-nullable field is being fed by a nullable field. */
  case object NullabilityViolation extends SchemaErrorType {
    override def name: String = "nullability-violation"
  }

  /** Generic schema incompatibility. */
  case object Incompatible extends SchemaErrorType {
    override def name: String = "incompatible"
  }
}

/**
 * A non-fatal schema validation warning.
 *
 * @param warningType The type of warning
 * @param message     Human-readable description
 * @param fieldName   The field name involved, if applicable
 */
final case class SchemaValidationWarning(
  warningType: SchemaWarningType,
  message: String,
  fieldName: Option[String] = None) {

  /** Formatted warning message. */
  def fullMessage: String =
    fieldName.map(f => s"$message (field: $f)").getOrElse(message)
}

/** Types of schema validation warnings. */
sealed trait SchemaWarningType {
  def name: String
}

object SchemaWarningType {

  /** Only one component in a pair has a schema contract. */
  case object PartialContract extends SchemaWarningType {
    override def name: String = "partial-contract"
  }

  /** Output has extra fields not consumed by the next component. */
  case object ExtraFields extends SchemaWarningType {
    override def name: String = "extra-fields"
  }

  /** Component should have a contract in strict mode. */
  case object MissingContract extends SchemaWarningType {
    override def name: String = "missing-contract"
  }
}

/**
 * Validates schema compatibility between adjacent components.
 */
object SchemaValidator {

  /**
   * Validate that an output schema is compatible with an input schema.
   *
   * Compatibility rules:
   *   - All fields required by input must exist in output
   *   - Field types must be compatible (exact match for now)
   *   - Non-nullable input fields require non-nullable output fields
   *
   * @param output The output schema from the producer component
   * @param input  The input schema expected by the consumer component
   * @return Validation result with errors and warnings
   */
  def validate(output: SchemaDefinition, input: SchemaDefinition): SchemaValidationResult = {
    val errors   = scala.collection.mutable.ListBuffer.empty[SchemaValidationError]
    val warnings = scala.collection.mutable.ListBuffer.empty[SchemaValidationWarning]

    // Check that all input fields exist in output
    input.fields.foreach { inputField =>
      output.field(inputField.name) match {
        case None =>
          errors += SchemaValidationError(
            errorType = SchemaErrorType.MissingField,
            message = s"Required field '${inputField.name}' is missing from producer output",
            fieldName = Some(inputField.name),
            expected = Some(inputField.dataType)
          )

        case Some(outputField) =>
          // Check type compatibility
          if (outputField.normalizedDataType != inputField.normalizedDataType) {
            errors += SchemaValidationError(
              errorType = SchemaErrorType.TypeMismatch,
              message = s"Field '${inputField.name}' has incompatible type",
              fieldName = Some(inputField.name),
              expected = Some(inputField.dataType),
              actual = Some(outputField.dataType)
            )
          }

          // Check nullability
          if (!inputField.nullable && outputField.nullable) {
            errors += SchemaValidationError(
              errorType = SchemaErrorType.NullabilityViolation,
              message = s"Field '${inputField.name}' is non-nullable in consumer but nullable in producer",
              fieldName = Some(inputField.name),
              expected = Some("non-nullable"),
              actual = Some("nullable")
            )
          }
      }
    }

    // Warn about extra fields in output not consumed by input
    val extraFields = output.fieldNames.filterNot(input.hasField)
    if (extraFields.nonEmpty) {
      warnings += SchemaValidationWarning(
        warningType = SchemaWarningType.ExtraFields,
        message = s"Producer output has ${extraFields.size} field(s) not consumed: ${extraFields.mkString(", ")}"
      )
    }

    if (errors.isEmpty) {
      SchemaValidationResult.Valid(warnings.toList)
    } else {
      SchemaValidationResult.Invalid(errors.toList, warnings.toList)
    }
  }

  /**
   * Validate schema contracts between two components.
   *
   * @param producer The component producing output
   * @param consumer The component consuming input
   * @param config   Schema validation configuration
   * @return Validation result
   */
  def validateComponents(
    producer: Option[SchemaContract],
    consumer: Option[SchemaContract],
    config: SchemaValidationConfig
  ): SchemaValidationResult = {
    if (!config.enabled) {
      return SchemaValidationResult.Skipped
    }

    val producerOutput = producer.flatMap(_.outputContract)
    val consumerInput  = consumer.flatMap(_.inputContract)

    (producerOutput, consumerInput) match {
      case (Some(output), Some(input)) =>
        validate(output, input)

      case (None, Some(_)) if config.strict =>
        SchemaValidationResult.Invalid(List(
          SchemaValidationError(
            errorType = SchemaErrorType.Incompatible,
            message = "Producer component does not declare an output schema (strict mode)"
          )
        ))

      case (Some(_), None) if config.strict =>
        SchemaValidationResult.Invalid(List(
          SchemaValidationError(
            errorType = SchemaErrorType.Incompatible,
            message = "Consumer component does not declare an input schema (strict mode)"
          )
        ))

      case (None, Some(_)) =>
        SchemaValidationResult.Valid(List(
          SchemaValidationWarning(
            warningType = SchemaWarningType.PartialContract,
            message = "Producer does not declare output schema; consumer input schema cannot be validated"
          )
        ))

      case (Some(_), None) =>
        SchemaValidationResult.Valid(List(
          SchemaValidationWarning(
            warningType = SchemaWarningType.PartialContract,
            message = "Consumer does not declare input schema; producer output schema will not be validated"
          )
        ))

      case (None, None) =>
        if (config.strict) {
          SchemaValidationResult.Invalid(List(
            SchemaValidationError(
              errorType = SchemaErrorType.Incompatible,
              message = "Neither component declares a schema contract (strict mode)"
            )
          ))
        } else {
          SchemaValidationResult.Skipped
        }
    }
  }
}
