package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import pureconfig._
import pureconfig.generic.auto._

/**
 * Tests for schema contract functionality.
 */
class SchemaContractSpec extends AnyFunSpec with Matchers {

  describe("SchemaField") {

    describe("construction") {

      it("should create a field with defaults") {
        val field = SchemaField("id", "string")

        field.name shouldBe "id"
        field.dataType shouldBe "string"
        field.nullable shouldBe true
        field.metadata shouldBe empty
      }

      it("should create a non-nullable field") {
        val field = SchemaField("id", "integer", nullable = false)

        field.nullable shouldBe false
      }

      it("should include metadata") {
        val field = SchemaField("amount", "decimal", metadata = Map("precision" -> "10", "scale" -> "2"))

        field.metadata should contain("precision" -> "10")
        field.metadata should contain("scale" -> "2")
      }
    }

    describe("normalizedDataType") {

      it("should lowercase the data type") {
        SchemaField("f", "STRING").normalizedDataType shouldBe "string"
        SchemaField("f", "INTEGER").normalizedDataType shouldBe "integer"
        SchemaField("f", "TimestamP").normalizedDataType shouldBe "timestamp"
      }

      it("should trim whitespace") {
        SchemaField("f", "  string  ").normalizedDataType shouldBe "string"
      }
    }
  }

  describe("SchemaDefinition") {

    describe("construction") {

      it("should create an empty schema") {
        val schema = SchemaDefinition.empty

        schema.fields shouldBe empty
        schema.description shouldBe None
      }

      it("should create a schema from fields") {
        val schema = SchemaDefinition(Seq(
          SchemaField("id", "string"),
          SchemaField("name", "string"),
          SchemaField("age", "integer")
        ))

        schema.fields should have size 3
      }

      it("should create a schema with description") {
        val schema = SchemaDefinition(
          Seq(SchemaField("id", "string")),
          Some("User identifier schema")
        )

        schema.description shouldBe Some("User identifier schema")
      }
    }

    describe("convenience methods") {

      val schema = SchemaDefinition(Seq(
        SchemaField("id", "string", nullable = false),
        SchemaField("name", "string"),
        SchemaField("age", "integer")
      ))

      it("should return field names") {
        schema.fieldNames shouldBe Seq("id", "name", "age")
      }

      it("should find field by name") {
        schema.field("name") shouldBe Some(SchemaField("name", "string"))
        schema.field("unknown") shouldBe None
      }

      it("should check if field exists") {
        schema.hasField("id") shouldBe true
        schema.hasField("unknown") shouldBe false
      }

      it("should return size") {
        schema.size shouldBe 3
      }
    }

    describe("of factory method") {

      it("should create schema from tuples") {
        val schema = SchemaDefinition.of(
          ("id", "string", false),
          ("name", "string", true),
          ("age", "integer", true)
        )

        schema.fields should have size 3
        schema.field("id").get.nullable shouldBe false
        schema.field("name").get.nullable shouldBe true
      }
    }
  }

  describe("SchemaValidationConfig") {

    describe("defaults") {

      it("should have validation disabled by default") {
        val config = SchemaValidationConfig()

        config.enabled shouldBe false
        config.strict shouldBe false
        config.failOnWarning shouldBe false
      }

      it("should provide an enabled preset") {
        val config = SchemaValidationConfig.Enabled

        config.enabled shouldBe true
        config.strict shouldBe false
      }

      it("should provide a strict preset") {
        val config = SchemaValidationConfig.Strict

        config.enabled shouldBe true
        config.strict shouldBe true
        config.failOnWarning shouldBe true
      }
    }

    describe("HOCON parsing") {

      it("should parse schema-validation config") {
        val hocon = ConfigFactory.parseString("""
          enabled = true
          strict = true
          fail-on-warning = true
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[SchemaValidationConfig]

        config.enabled shouldBe true
        config.strict shouldBe true
        config.failOnWarning shouldBe true
      }

      it("should use defaults for missing fields") {
        val hocon = ConfigFactory.parseString("""
          enabled = true
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[SchemaValidationConfig]

        config.enabled shouldBe true
        config.strict shouldBe false
        config.failOnWarning shouldBe false
      }
    }
  }

  describe("SchemaValidator") {

    describe("validate") {

      it("should pass when schemas match exactly") {
        val output = SchemaDefinition(Seq(
          SchemaField("id", "string"),
          SchemaField("name", "string")
        ))
        val input = SchemaDefinition(Seq(
          SchemaField("id", "string"),
          SchemaField("name", "string")
        ))

        val result = SchemaValidator.validate(output, input)

        result.isValid shouldBe true
        result.errors shouldBe empty
      }

      it("should pass when output has extra fields") {
        val output = SchemaDefinition(Seq(
          SchemaField("id", "string"),
          SchemaField("name", "string"),
          SchemaField("extra", "integer")
        ))
        val input = SchemaDefinition(Seq(
          SchemaField("id", "string"),
          SchemaField("name", "string")
        ))

        val result = SchemaValidator.validate(output, input)

        result.isValid shouldBe true
        result.warnings should have size 1
        result.warnings.head.warningType shouldBe SchemaWarningType.ExtraFields
      }

      it("should fail when output is missing required fields") {
        val output = SchemaDefinition(Seq(
          SchemaField("id", "string")
        ))
        val input = SchemaDefinition(Seq(
          SchemaField("id", "string"),
          SchemaField("name", "string")
        ))

        val result = SchemaValidator.validate(output, input)

        result.isValid shouldBe false
        result.errors should have size 1
        result.errors.head.errorType shouldBe SchemaErrorType.MissingField
        result.errors.head.fieldName shouldBe Some("name")
      }

      it("should fail when field types don't match") {
        val output = SchemaDefinition(Seq(
          SchemaField("id", "string"),
          SchemaField("count", "string")
        ))
        val input = SchemaDefinition(Seq(
          SchemaField("id", "string"),
          SchemaField("count", "integer")
        ))

        val result = SchemaValidator.validate(output, input)

        result.isValid shouldBe false
        result.errors should have size 1
        result.errors.head.errorType shouldBe SchemaErrorType.TypeMismatch
        result.errors.head.fieldName shouldBe Some("count")
      }

      it("should fail when non-nullable field is fed by nullable") {
        val output = SchemaDefinition(Seq(
          SchemaField("id", "string", nullable = true)
        ))
        val input = SchemaDefinition(Seq(
          SchemaField("id", "string", nullable = false)
        ))

        val result = SchemaValidator.validate(output, input)

        result.isValid shouldBe false
        result.errors should have size 1
        result.errors.head.errorType shouldBe SchemaErrorType.NullabilityViolation
      }

      it("should pass when nullable field is fed by non-nullable") {
        val output = SchemaDefinition(Seq(
          SchemaField("id", "string", nullable = false)
        ))
        val input = SchemaDefinition(Seq(
          SchemaField("id", "string", nullable = true)
        ))

        val result = SchemaValidator.validate(output, input)

        result.isValid shouldBe true
      }

      it("should collect multiple errors") {
        val output = SchemaDefinition(Seq(
          SchemaField("id", "integer")  // Wrong type
        ))
        val input = SchemaDefinition(Seq(
          SchemaField("id", "string"),
          SchemaField("name", "string"),  // Missing
          SchemaField("age", "integer")   // Missing
        ))

        val result = SchemaValidator.validate(output, input)

        result.isValid shouldBe false
        result.errors should have size 3
      }

      it("should handle case-insensitive type comparison") {
        val output = SchemaDefinition(Seq(
          SchemaField("id", "STRING")
        ))
        val input = SchemaDefinition(Seq(
          SchemaField("id", "string")
        ))

        val result = SchemaValidator.validate(output, input)

        result.isValid shouldBe true
      }
    }

    describe("validateComponents") {

      trait TestContract extends SchemaContract

      it("should skip validation when disabled") {
        val config = SchemaValidationConfig(enabled = false)

        val result = SchemaValidator.validateComponents(None, None, config)

        result shouldBe SchemaValidationResult.Skipped
      }

      it("should skip when neither component has contracts") {
        val config = SchemaValidationConfig(enabled = true)

        val result = SchemaValidator.validateComponents(None, None, config)

        result shouldBe SchemaValidationResult.Skipped
      }

      it("should warn when only producer has contract") {
        val producer = new TestContract {
          override def outputContract: Option[SchemaDefinition] = Some(
            SchemaDefinition(Seq(SchemaField("id", "string")))
          )
        }
        val config = SchemaValidationConfig(enabled = true)

        val result = SchemaValidator.validateComponents(Some(producer), None, config)

        result.isValid shouldBe true
        result.warnings should have size 1
        result.warnings.head.warningType shouldBe SchemaWarningType.PartialContract
      }

      it("should warn when only consumer has contract") {
        val consumer = new TestContract {
          override def inputContract: Option[SchemaDefinition] = Some(
            SchemaDefinition(Seq(SchemaField("id", "string")))
          )
        }
        val config = SchemaValidationConfig(enabled = true)

        val result = SchemaValidator.validateComponents(None, Some(consumer), config)

        result.isValid shouldBe true
        result.warnings should have size 1
      }

      it("should validate when both have contracts") {
        val producer = new TestContract {
          override def outputContract: Option[SchemaDefinition] = Some(
            SchemaDefinition(Seq(SchemaField("id", "string")))
          )
        }
        val consumer = new TestContract {
          override def inputContract: Option[SchemaDefinition] = Some(
            SchemaDefinition(Seq(SchemaField("id", "string")))
          )
        }
        val config = SchemaValidationConfig(enabled = true)

        val result = SchemaValidator.validateComponents(Some(producer), Some(consumer), config)

        result.isValid shouldBe true
      }

      it("should fail in strict mode when producer lacks contract") {
        val consumer = new TestContract {
          override def inputContract: Option[SchemaDefinition] = Some(
            SchemaDefinition(Seq(SchemaField("id", "string")))
          )
        }
        val config = SchemaValidationConfig(enabled = true, strict = true)

        val result = SchemaValidator.validateComponents(None, Some(consumer), config)

        result.isValid shouldBe false
      }

      it("should fail in strict mode when consumer lacks contract") {
        val producer = new TestContract {
          override def outputContract: Option[SchemaDefinition] = Some(
            SchemaDefinition(Seq(SchemaField("id", "string")))
          )
        }
        val config = SchemaValidationConfig(enabled = true, strict = true)

        val result = SchemaValidator.validateComponents(Some(producer), None, config)

        result.isValid shouldBe false
      }

      it("should fail in strict mode when neither has contracts") {
        val config = SchemaValidationConfig(enabled = true, strict = true)

        val result = SchemaValidator.validateComponents(None, None, config)

        result.isValid shouldBe false
      }
    }
  }

  describe("SchemaValidationError") {

    it("should format full message with all details") {
      val error = SchemaValidationError(
        errorType = SchemaErrorType.TypeMismatch,
        message = "Field has incompatible type",
        fieldName = Some("count"),
        expected = Some("integer"),
        actual = Some("string")
      )

      error.fullMessage should include("Field has incompatible type")
      error.fullMessage should include("field: count")
      error.fullMessage should include("expected: integer")
      error.fullMessage should include("actual: string")
    }

    it("should format message without optional details") {
      val error = SchemaValidationError(
        errorType = SchemaErrorType.Incompatible,
        message = "Schemas are incompatible"
      )

      error.fullMessage shouldBe "Schemas are incompatible"
    }
  }

  describe("SchemaValidationWarning") {

    it("should format message with field name") {
      val warning = SchemaValidationWarning(
        warningType = SchemaWarningType.ExtraFields,
        message = "Extra field not consumed",
        fieldName = Some("extra_column")
      )

      warning.fullMessage should include("extra_column")
    }

    it("should format message without field name") {
      val warning = SchemaValidationWarning(
        warningType = SchemaWarningType.PartialContract,
        message = "Only one component has contract"
      )

      warning.fullMessage shouldBe "Only one component has contract"
    }
  }

  describe("PipelineConfig with schema-validation") {

    it("should parse pipeline config with schema-validation") {
      val hocon = ConfigFactory.parseString("""
        pipeline-name = "Test Pipeline"
        schema-validation {
          enabled = true
          strict = false
          fail-on-warning = true
        }
        pipeline-components = []
      """)

      val config = ConfigSource.fromConfig(hocon).loadOrThrow[PipelineConfig]

      config.schemaValidation shouldBe defined
      config.schemaValidation.get.enabled shouldBe true
      config.schemaValidation.get.strict shouldBe false
      config.schemaValidation.get.failOnWarning shouldBe true
    }

    it("should use None when schema-validation is not specified") {
      val hocon = ConfigFactory.parseString("""
        pipeline-name = "Test Pipeline"
        pipeline-components = []
      """)

      val config = ConfigSource.fromConfig(hocon).loadOrThrow[PipelineConfig]

      config.schemaValidation shouldBe None
    }
  }
}
