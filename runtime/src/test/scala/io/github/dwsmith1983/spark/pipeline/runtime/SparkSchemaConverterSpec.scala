package io.github.dwsmith1983.spark.pipeline.runtime

import io.github.dwsmith1983.spark.pipeline.config.{SchemaDefinition, SchemaField, SparkConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/**
 * Tests for SparkSchemaConverter.
 */
class SparkSchemaConverterSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val config: SparkConfig = SparkConfig(
      master = Some("local[2]"),
      appName = Some("SparkSchemaConverterTest"),
      config = Map(
        "spark.ui.enabled"             -> "false",
        "spark.sql.shuffle.partitions" -> "2"
      )
    )
    spark = SparkSessionWrapper.configure(config)
  }

  override def afterAll(): Unit =
    SparkSessionWrapper.stop()

  describe("SparkSchemaConverter") {

    describe("toSparkType") {

      it("should convert string type") {
        SparkSchemaConverter.toSparkType("string") shouldBe StringType
        SparkSchemaConverter.toSparkType("STRING") shouldBe StringType
      }

      it("should convert integer types") {
        SparkSchemaConverter.toSparkType("integer") shouldBe IntegerType
        SparkSchemaConverter.toSparkType("int") shouldBe IntegerType
      }

      it("should convert long types") {
        SparkSchemaConverter.toSparkType("long") shouldBe LongType
        SparkSchemaConverter.toSparkType("bigint") shouldBe LongType
      }

      it("should convert double type") {
        SparkSchemaConverter.toSparkType("double") shouldBe DoubleType
      }

      it("should convert float type") {
        SparkSchemaConverter.toSparkType("float") shouldBe FloatType
      }

      it("should convert boolean types") {
        SparkSchemaConverter.toSparkType("boolean") shouldBe BooleanType
        SparkSchemaConverter.toSparkType("bool") shouldBe BooleanType
      }

      it("should convert timestamp type") {
        SparkSchemaConverter.toSparkType("timestamp") shouldBe TimestampType
      }

      it("should convert date type") {
        SparkSchemaConverter.toSparkType("date") shouldBe DateType
      }

      it("should convert binary type") {
        SparkSchemaConverter.toSparkType("binary") shouldBe BinaryType
      }

      it("should convert byte types") {
        SparkSchemaConverter.toSparkType("byte") shouldBe ByteType
        SparkSchemaConverter.toSparkType("tinyint") shouldBe ByteType
      }

      it("should convert short types") {
        SparkSchemaConverter.toSparkType("short") shouldBe ShortType
        SparkSchemaConverter.toSparkType("smallint") shouldBe ShortType
      }

      it("should convert decimal type with precision and scale") {
        val decimalType = SparkSchemaConverter.toSparkType("decimal(10,2)")
        decimalType shouldBe a[DecimalType]
        decimalType.asInstanceOf[DecimalType].precision shouldBe 10
        decimalType.asInstanceOf[DecimalType].scale shouldBe 2
      }

      it("should convert decimal type without precision") {
        val decimalType = SparkSchemaConverter.toSparkType("decimal")
        decimalType shouldBe a[DecimalType]
      }

      it("should throw on unsupported type") {
        val exception = intercept[IllegalArgumentException] {
          SparkSchemaConverter.toSparkType("unsupported_type")
        }
        exception.getMessage should include("Unsupported data type")
      }
    }

    describe("fromSparkType") {

      it("should convert StringType") {
        SparkSchemaConverter.fromSparkType(StringType) shouldBe "string"
      }

      it("should convert IntegerType") {
        SparkSchemaConverter.fromSparkType(IntegerType) shouldBe "integer"
      }

      it("should convert LongType") {
        SparkSchemaConverter.fromSparkType(LongType) shouldBe "long"
      }

      it("should convert DoubleType") {
        SparkSchemaConverter.fromSparkType(DoubleType) shouldBe "double"
      }

      it("should convert FloatType") {
        SparkSchemaConverter.fromSparkType(FloatType) shouldBe "float"
      }

      it("should convert BooleanType") {
        SparkSchemaConverter.fromSparkType(BooleanType) shouldBe "boolean"
      }

      it("should convert TimestampType") {
        SparkSchemaConverter.fromSparkType(TimestampType) shouldBe "timestamp"
      }

      it("should convert DateType") {
        SparkSchemaConverter.fromSparkType(DateType) shouldBe "date"
      }

      it("should convert DecimalType with precision and scale") {
        SparkSchemaConverter.fromSparkType(DecimalType(18, 4)) shouldBe "decimal(18,4)"
      }

      it("should convert ArrayType") {
        SparkSchemaConverter.fromSparkType(ArrayType(StringType)) shouldBe "array<string>"
      }

      it("should convert MapType") {
        SparkSchemaConverter.fromSparkType(MapType(StringType, IntegerType)) shouldBe "map<string,integer>"
      }
    }

    describe("toStructField") {

      it("should convert a nullable field") {
        val field = SchemaField("name", "string", nullable = true)
        val structField = SparkSchemaConverter.toStructField(field)

        structField.name shouldBe "name"
        structField.dataType shouldBe StringType
        structField.nullable shouldBe true
      }

      it("should convert a non-nullable field") {
        val field = SchemaField("id", "integer", nullable = false)
        val structField = SparkSchemaConverter.toStructField(field)

        structField.name shouldBe "id"
        structField.dataType shouldBe IntegerType
        structField.nullable shouldBe false
      }

      it("should include metadata") {
        val field = SchemaField("amount", "decimal", metadata = Map("precision" -> "10"))
        val structField = SparkSchemaConverter.toStructField(field)

        structField.metadata.getString("precision") shouldBe "10"
      }
    }

    describe("fromStructField") {

      it("should convert a StructField to SchemaField") {
        val structField = StructField("name", StringType, nullable = true)
        val schemaField = SparkSchemaConverter.fromStructField(structField)

        schemaField.name shouldBe "name"
        schemaField.dataType shouldBe "string"
        schemaField.nullable shouldBe true
      }

      it("should handle non-nullable fields") {
        val structField = StructField("id", LongType, nullable = false)
        val schemaField = SparkSchemaConverter.fromStructField(structField)

        schemaField.nullable shouldBe false
      }
    }

    describe("toStructType") {

      it("should convert a SchemaDefinition to StructType") {
        val schema = SchemaDefinition(Seq(
          SchemaField("id", "long", nullable = false),
          SchemaField("name", "string"),
          SchemaField("age", "integer")
        ))

        val structType = SparkSchemaConverter.toStructType(schema)

        structType.fields should have size 3
        structType("id").dataType shouldBe LongType
        structType("id").nullable shouldBe false
        structType("name").dataType shouldBe StringType
        structType("age").dataType shouldBe IntegerType
      }

      it("should handle empty schema") {
        val schema = SchemaDefinition.empty
        val structType = SparkSchemaConverter.toStructType(schema)

        structType.fields shouldBe empty
      }
    }

    describe("fromStructType") {

      it("should convert a StructType to SchemaDefinition") {
        val structType = StructType(Seq(
          StructField("id", LongType, nullable = false),
          StructField("name", StringType),
          StructField("amount", DecimalType(10, 2))
        ))

        val schema = SparkSchemaConverter.fromStructType(structType)

        schema.fields should have size 3
        schema.field("id").get.dataType shouldBe "long"
        schema.field("id").get.nullable shouldBe false
        schema.field("amount").get.dataType shouldBe "decimal(10,2)"
      }

      it("should include description if provided") {
        val structType = StructType(Seq(StructField("id", LongType)))
        val schema = SparkSchemaConverter.fromStructType(structType, Some("User table"))

        schema.description shouldBe Some("User table")
      }
    }

    describe("isCompatible") {

      it("should return true when schemas match") {
        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long"),
          SchemaField("name", "string")
        ))
        val actual = StructType(Seq(
          StructField("id", LongType),
          StructField("name", StringType)
        ))

        SparkSchemaConverter.isCompatible(expected, actual) shouldBe true
      }

      it("should return true when actual has extra fields") {
        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long")
        ))
        val actual = StructType(Seq(
          StructField("id", LongType),
          StructField("extra", StringType)
        ))

        SparkSchemaConverter.isCompatible(expected, actual) shouldBe true
      }

      it("should return false when expected field is missing") {
        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long"),
          SchemaField("name", "string")
        ))
        val actual = StructType(Seq(
          StructField("id", LongType)
        ))

        SparkSchemaConverter.isCompatible(expected, actual) shouldBe false
      }

      it("should return false when types don't match") {
        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long")
        ))
        val actual = StructType(Seq(
          StructField("id", StringType)
        ))

        SparkSchemaConverter.isCompatible(expected, actual) shouldBe false
      }

      it("should return false when nullability is violated") {
        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long", nullable = false)
        ))
        val actual = StructType(Seq(
          StructField("id", LongType, nullable = true)
        ))

        SparkSchemaConverter.isCompatible(expected, actual) shouldBe false
      }

      it("should handle type aliases") {
        val expected = SchemaDefinition(Seq(
          SchemaField("id", "int"),
          SchemaField("count", "bigint")
        ))
        val actual = StructType(Seq(
          StructField("id", IntegerType),
          StructField("count", LongType)
        ))

        SparkSchemaConverter.isCompatible(expected, actual) shouldBe true
      }
    }

    describe("validateAgainst") {

      it("should return empty list when valid") {
        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long"),
          SchemaField("name", "string")
        ))
        val actual = StructType(Seq(
          StructField("id", LongType),
          StructField("name", StringType)
        ))

        SparkSchemaConverter.validateAgainst(expected, actual) shouldBe empty
      }

      it("should return errors for missing fields") {
        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long"),
          SchemaField("name", "string")
        ))
        val actual = StructType(Seq(
          StructField("id", LongType)
        ))

        val errors = SparkSchemaConverter.validateAgainst(expected, actual)

        errors should have size 1
        errors.head should include("Missing field")
        errors.head should include("name")
      }

      it("should return errors for type mismatches") {
        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long")
        ))
        val actual = StructType(Seq(
          StructField("id", StringType)
        ))

        val errors = SparkSchemaConverter.validateAgainst(expected, actual)

        errors should have size 1
        errors.head should include("Type mismatch")
      }

      it("should return errors for nullability violations") {
        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long", nullable = false)
        ))
        val actual = StructType(Seq(
          StructField("id", LongType, nullable = true)
        ))

        val errors = SparkSchemaConverter.validateAgainst(expected, actual)

        errors should have size 1
        errors.head should include("Nullability violation")
      }

      it("should collect multiple errors") {
        val expected = SchemaDefinition(Seq(
          SchemaField("id", "string"),  // Wrong type
          SchemaField("missing", "long")  // Missing
        ))
        val actual = StructType(Seq(
          StructField("id", LongType)
        ))

        val errors = SparkSchemaConverter.validateAgainst(expected, actual)

        errors should have size 2
      }
    }

    describe("round-trip conversion") {

      it("should preserve schema through round-trip") {
        val original = SchemaDefinition(Seq(
          SchemaField("id", "long", nullable = false),
          SchemaField("name", "string"),
          SchemaField("amount", "double"),
          SchemaField("active", "boolean")
        ))

        val structType = SparkSchemaConverter.toStructType(original)
        val roundTrip = SparkSchemaConverter.fromStructType(structType)

        roundTrip.fieldNames shouldBe original.fieldNames
        roundTrip.fields.zip(original.fields).foreach { case (rt, orig) =>
          rt.name shouldBe orig.name
          rt.nullable shouldBe orig.nullable
        }
      }
    }

    describe("integration with real DataFrames") {

      it("should validate actual DataFrame schema") {
        val df = spark.sql("""
          SELECT
            1L as id,
            'Alice' as name,
            25 as age
        """)

        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long"),
          SchemaField("name", "string"),
          SchemaField("age", "integer")
        ))

        SparkSchemaConverter.isCompatible(expected, df.schema) shouldBe true
      }

      it("should detect missing columns in DataFrame") {
        val df = spark.sql("SELECT 1L as id")

        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long"),
          SchemaField("name", "string")
        ))

        SparkSchemaConverter.isCompatible(expected, df.schema) shouldBe false
      }

      it("should work with complex schemas") {
        val df = spark.sql("""
          SELECT
            1L as id,
            'test' as name,
            cast(10.5 as double) as amount,
            true as active,
            current_timestamp() as created_at
        """)

        val expected = SchemaDefinition(Seq(
          SchemaField("id", "long"),
          SchemaField("name", "string"),
          SchemaField("amount", "double"),
          SchemaField("active", "boolean"),
          SchemaField("created_at", "timestamp")
        ))

        SparkSchemaConverter.isCompatible(expected, df.schema) shouldBe true
      }
    }
  }
}
