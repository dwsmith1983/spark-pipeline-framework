package io.github.dwsmith1983.spark.pipeline.runtime

import io.github.dwsmith1983.spark.pipeline.config.{SchemaDefinition, SchemaField}
import org.apache.spark.sql.types._

/**
 * Converts between platform-independent schema definitions and Spark StructType.
 *
 * This converter enables schema contracts defined in the core module to be
 * validated against actual Spark DataFrame schemas at runtime.
 *
 * == Supported Data Types ==
 *
 * The following data type strings are recognized:
 *
 * | String          | Spark Type       |
 * |-----------------|------------------|
 * | string          | StringType       |
 * | integer, int    | IntegerType      |
 * | long, bigint    | LongType         |
 * | double          | DoubleType       |
 * | float           | FloatType        |
 * | boolean, bool   | BooleanType      |
 * | timestamp       | TimestampType    |
 * | date            | DateType         |
 * | binary          | BinaryType       |
 * | byte, tinyint   | ByteType         |
 * | short, smallint | ShortType        |
 * | decimal(p,s)    | DecimalType(p,s) |
 *
 * Complex types (array, map, struct) are partially supported for conversion
 * from Spark to SchemaDefinition, but not yet for the reverse direction.
 */
object SparkSchemaConverter {

  /**
   * Converts a SchemaDefinition to a Spark StructType.
   *
   * @param schema The platform-independent schema definition
   * @return The equivalent Spark StructType
   */
  def toStructType(schema: SchemaDefinition): StructType =
    StructType(schema.fields.map(toStructField))

  /**
   * Converts a SchemaField to a Spark StructField.
   *
   * @param field The platform-independent field definition
   * @return The equivalent Spark StructField
   */
  def toStructField(field: SchemaField): StructField = {
    val sparkType = toSparkType(field.normalizedDataType)
    val metadata = if (field.metadata.isEmpty) {
      Metadata.empty
    } else {
      val builder = new MetadataBuilder()
      field.metadata.foreach { case (k, v) => builder.putString(k, v) }
      builder.build()
    }
    StructField(field.name, sparkType, field.nullable, metadata)
  }

  /**
   * Converts a data type string to a Spark DataType.
   *
   * @param dataType The data type string (e.g., "string", "integer", "decimal(10,2)")
   * @return The equivalent Spark DataType
   */
  def toSparkType(dataType: String): DataType = {
    val normalized = dataType.toLowerCase.trim

    normalized match {
      case "string"                                 => StringType
      case "integer" | "int"                        => IntegerType
      case "long" | "bigint"                        => LongType
      case "double"                                 => DoubleType
      case "float"                                  => FloatType
      case "boolean" | "bool"                       => BooleanType
      case "timestamp"                              => TimestampType
      case "date"                                   => DateType
      case "binary"                                 => BinaryType
      case "byte" | "tinyint"                       => ByteType
      case "short" | "smallint"                     => ShortType
      case decimal if decimal.startsWith("decimal") => parseDecimalType(decimal)
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported data type: $dataType. " +
            "Supported types: string, integer, long, double, float, boolean, " +
            "timestamp, date, binary, byte, short, decimal(precision,scale)"
        )
    }
  }

  /**
   * Converts a Spark StructType to a SchemaDefinition.
   *
   * @param structType The Spark StructType
   * @param description Optional description for the schema
   * @return The equivalent platform-independent SchemaDefinition
   */
  def fromStructType(structType: StructType, description: Option[String] = None): SchemaDefinition =
    SchemaDefinition(
      fields = structType.fields.map(fromStructField).toSeq,
      description = description
    )

  /**
   * Converts a Spark StructField to a SchemaField.
   *
   * @param field The Spark StructField
   * @return The equivalent platform-independent SchemaField
   */
  def fromStructField(field: StructField): SchemaField = {
    val metadata: Map[String, String] = {
      val meta = field.metadata
      if (meta == Metadata.empty) {
        Map.empty
      } else {
        // Extract string values from metadata (limited support)
        val builder = Map.newBuilder[String, String]
        try {
          val json = meta.json
          if (json != "{}") {
            builder += ("_raw" -> json)
          }
        } catch {
          case _: Exception => // Ignore metadata extraction errors
        }
        builder.result()
      }
    }

    SchemaField(
      name = field.name,
      dataType = fromSparkType(field.dataType),
      nullable = field.nullable,
      metadata = metadata
    )
  }

  /**
   * Converts a Spark DataType to a data type string.
   *
   * @param dataType The Spark DataType
   * @return The equivalent data type string
   */
  def fromSparkType(dataType: DataType): String = dataType match {
    case StringType         => "string"
    case IntegerType        => "integer"
    case LongType           => "long"
    case DoubleType         => "double"
    case FloatType          => "float"
    case BooleanType        => "boolean"
    case TimestampType      => "timestamp"
    case DateType           => "date"
    case BinaryType         => "binary"
    case ByteType           => "byte"
    case ShortType          => "short"
    case d: DecimalType     => s"decimal(${d.precision},${d.scale})"
    case ArrayType(et, _)   => s"array<${fromSparkType(et)}>"
    case MapType(kt, vt, _) => s"map<${fromSparkType(kt)},${fromSparkType(vt)}>"
    case st: StructType     => s"struct<${st.fields.map(f => s"${f.name}:${fromSparkType(f.dataType)}").mkString(",")}>"
    case _                  => dataType.typeName
  }

  /**
   * Checks if a Spark DataFrame's schema is compatible with an expected SchemaDefinition.
   *
   * Compatibility means:
   *   - All expected fields exist in the actual schema
   *   - Field types match (using normalized comparison)
   *   - Nullability constraints are satisfied
   *
   * @param expected The expected schema definition
   * @param actual   The actual Spark StructType
   * @return true if compatible, false otherwise
   */
  def isCompatible(expected: SchemaDefinition, actual: StructType): Boolean = {
    val actualFields = actual.fields.map(f => f.name -> f).toMap

    expected.fields.forall { expectedField =>
      actualFields.get(expectedField.name) match {
        case None => false
        case Some(actualField) =>
          val typeMatch = normalizeTypeName(fromSparkType(actualField.dataType)) ==
            normalizeTypeName(expectedField.dataType)
          val nullabilityOk = expectedField.nullable || !actualField.nullable
          typeMatch && nullabilityOk
      }
    }
  }

  /**
   * Validates an actual Spark StructType against an expected SchemaDefinition.
   *
   * @param expected The expected schema definition
   * @param actual   The actual Spark StructType
   * @return List of validation error messages (empty if valid)
   */
  def validateAgainst(expected: SchemaDefinition, actual: StructType): List[String] = {
    val errors = scala.collection.mutable.ListBuffer.empty[String]
    val actualFields = actual.fields.map(f => f.name -> f).toMap

    expected.fields.foreach { expectedField =>
      actualFields.get(expectedField.name) match {
        case None =>
          errors += s"Missing field: ${expectedField.name} (expected type: ${expectedField.dataType})"

        case Some(actualField) =>
          val actualTypeName = fromSparkType(actualField.dataType)
          if (normalizeTypeName(actualTypeName) != normalizeTypeName(expectedField.dataType)) {
            errors += s"Type mismatch for field '${expectedField.name}': " +
              s"expected ${expectedField.dataType}, found $actualTypeName"
          }

          if (!expectedField.nullable && actualField.nullable) {
            errors += s"Nullability violation for field '${expectedField.name}': " +
              "expected non-nullable, but field is nullable"
          }
      }
    }

    errors.toList
  }

  private def parseDecimalType(decimalStr: String): DecimalType = {
    val pattern = """decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r
    decimalStr match {
      case pattern(precision, scale) =>
        DecimalType(precision.toInt, scale.toInt)
      case "decimal" =>
        DecimalType.SYSTEM_DEFAULT
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid decimal type format: $decimalStr. Expected: decimal(precision,scale)"
        )
    }
  }

  private def normalizeTypeName(typeName: String): String = {
    val normalized = typeName.toLowerCase.trim
    normalized match {
      case "int"      => "integer"
      case "bigint"   => "long"
      case "bool"     => "boolean"
      case "tinyint"  => "byte"
      case "smallint" => "short"
      case other      => other
    }
  }
}
