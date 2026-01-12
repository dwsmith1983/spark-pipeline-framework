package io.github.dwsmith1983.spark.pipeline.dq

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Base trait for data quality checks.
 *
 * A DataQualityCheck validates data in a Spark table or view and returns
 * a result indicating whether the check passed, failed, or was skipped.
 *
 * == Implementing Custom Checks ==
 *
 * {{{
 * class MyCustomCheck(table: String, threshold: Double) extends DataQualityCheck {
 *   override def name: String = "my_custom_check"
 *   override def tableName: String = table
 *
 *   override def run(spark: SparkSession): DataQualityResult = {
 *     if (!tableExists(spark)) {
 *       return DataQualityResult.Skipped(name, tableName, s"Table '\$tableName' not found")
 *     }
 *
 *     val df = spark.table(tableName)
 *     // Perform validation logic...
 *
 *     if (validationPassed) {
 *       DataQualityResult.Passed(name, tableName, Map("threshold" -> threshold))
 *     } else {
 *       DataQualityResult.Failed(name, tableName, "Validation failed", Map("actual" -> actual))
 *     }
 *   }
 * }
 * }}}
 */
trait DataQualityCheck {

  /** Unique name identifying this check type. */
  def name: String

  /** Name of the table or view to check. */
  def tableName: String

  /**
   * Execute the data quality check.
   *
   * @param spark The SparkSession to use for querying
   * @return Result indicating pass, fail, warning, or skipped
   */
  def run(spark: SparkSession): DataQualityResult

  /**
   * Helper to check if the target table exists.
   *
   * @param spark The SparkSession
   * @return true if table exists, false otherwise
   */
  protected def tableExists(spark: SparkSession): Boolean =
    spark.catalog.tableExists(tableName)

  /**
   * Helper to safely get the table as a DataFrame.
   * Returns None if the table doesn't exist.
   */
  protected def getTable(spark: SparkSession): Option[DataFrame] =
    if (tableExists(spark)) Some(spark.table(tableName)) else None
}

/**
 * Validates that a table has at least a minimum number of rows.
 *
 * == Example ==
 *
 * {{{
 * // Ensure table has at least 1000 rows
 * RowCountCheck("raw_data", minRows = 1000)
 *
 * // Ensure table has between 100 and 10000 rows
 * RowCountCheck("processed_data", minRows = 100, maxRows = Some(10000))
 * }}}
 *
 * @param tableName Name of the table to check
 * @param minRows Minimum number of rows required (default: 1)
 * @param maxRows Optional maximum number of rows allowed
 */
final case class RowCountCheck(
  tableName: String,
  minRows: Long = 1,
  maxRows: Option[Long] = None)
  extends DataQualityCheck {

  require(minRows >= 0, "minRows must be non-negative")
  maxRows.foreach(max => require(max >= minRows, "maxRows must be >= minRows"))

  override val name: String = "row_count"

  override def run(spark: SparkSession): DataQualityResult = {
    if (!tableExists(spark)) {
      return DataQualityResult.Skipped(name, tableName, s"Table '$tableName' not found")
    }

    val count = spark.table(tableName).count()
    val details = Map[String, Any](
      "actual_count" -> count,
      "min_required" -> minRows
    ) ++ maxRows.map("max_allowed" -> _)

    if (count < minRows) {
      DataQualityResult.Failed(
        name,
        tableName,
        s"Row count $count is below minimum $minRows",
        details
      )
    } else if (maxRows.exists(count > _)) {
      DataQualityResult.Failed(
        name,
        tableName,
        s"Row count $count exceeds maximum ${maxRows.get}",
        details
      )
    } else {
      DataQualityResult.Passed(name, tableName, details)
    }
  }
}

/**
 * Validates that specified columns have null values within acceptable limits.
 *
 * == Example ==
 *
 * {{{
 * // Ensure user_id column has no nulls
 * NullCheck("users", columns = Seq("user_id"), maxNullPercent = 0.0)
 *
 * // Allow up to 5% nulls in optional_field
 * NullCheck("events", columns = Seq("optional_field"), maxNullPercent = 5.0)
 * }}}
 *
 * @param tableName Name of the table to check
 * @param columns Columns to check for null values
 * @param maxNullPercent Maximum allowed percentage of null values (0.0 to 100.0)
 */
final case class NullCheck(
  tableName: String,
  columns: Seq[String],
  maxNullPercent: Double = 0.0)
  extends DataQualityCheck {

  require(columns.nonEmpty, "columns must not be empty")
  require(maxNullPercent >= 0.0 && maxNullPercent <= 100.0, "maxNullPercent must be between 0 and 100")

  override val name: String = "null_check"

  override def run(spark: SparkSession): DataQualityResult = {
    if (!tableExists(spark)) {
      return DataQualityResult.Skipped(name, tableName, s"Table '$tableName' not found")
    }

    val df         = spark.table(tableName)
    val totalCount = df.count()

    if (totalCount == 0) {
      return DataQualityResult.Passed(
        name,
        tableName,
        Map("total_rows" -> 0L, "columns_checked" -> columns.mkString(","))
      )
    }

    val nullCounts = columns.map { col =>
      val nullCount = df.filter(df(col).isNull).count()
      val nullPct   = (nullCount.toDouble / totalCount) * 100.0
      (col, nullCount, nullPct)
    }

    val failures = nullCounts.filter(_._3 > maxNullPercent)

    val details: Map[String, Any] = Map[String, Any](
      "total_rows"       -> totalCount,
      "columns_checked"  -> columns.mkString(","),
      "max_null_percent" -> maxNullPercent
    ) ++ nullCounts.map { case (col, count, _) =>
      s"${col}_null_count" -> (count: Any)
    }.toMap ++ nullCounts.map { case (col, _, pct) =>
      s"${col}_null_percent" -> (f"$pct%.2f": Any)
    }.toMap

    if (failures.nonEmpty) {
      val failureMsg = failures
        .map { case (col, _, pct) => s"$col: ${f"$pct%.2f"}%" }
        .mkString(", ")
      DataQualityResult.Failed(
        name,
        tableName,
        s"Null percentage exceeds $maxNullPercent% in: $failureMsg",
        details
      )
    } else {
      DataQualityResult.Passed(name, tableName, details)
    }
  }
}

/**
 * Validates that a table contains all required columns.
 *
 * == Example ==
 *
 * {{{
 * // Ensure required columns exist
 * SchemaCheck("output", requiredColumns = Set("id", "name", "timestamp"))
 * }}}
 *
 * @param tableName Name of the table to check
 * @param requiredColumns Set of column names that must exist
 */
final case class SchemaCheck(
  tableName: String,
  requiredColumns: Set[String])
  extends DataQualityCheck {

  require(requiredColumns.nonEmpty, "requiredColumns must not be empty")

  override val name: String = "schema_check"

  override def run(spark: SparkSession): DataQualityResult = {
    if (!tableExists(spark)) {
      return DataQualityResult.Skipped(name, tableName, s"Table '$tableName' not found")
    }

    val df             = spark.table(tableName)
    val actualColumns  = df.columns.toSet
    val missingColumns = requiredColumns -- actualColumns

    val details = Map[String, Any](
      "required_columns" -> requiredColumns.mkString(","),
      "actual_columns"   -> actualColumns.mkString(","),
      "missing_columns"  -> missingColumns.mkString(",")
    )

    if (missingColumns.nonEmpty) {
      DataQualityResult.Failed(
        name,
        tableName,
        s"Missing required columns: ${missingColumns.mkString(", ")}",
        details
      )
    } else {
      DataQualityResult.Passed(name, tableName, details)
    }
  }
}

/**
 * Validates that specified columns contain unique values (no duplicates).
 *
 * == Example ==
 *
 * {{{
 * // Ensure user_id is unique
 * UniqueCheck("users", columns = Seq("user_id"))
 *
 * // Ensure composite key is unique
 * UniqueCheck("events", columns = Seq("event_date", "event_id"))
 * }}}
 *
 * @param tableName Name of the table to check
 * @param columns Columns that should form a unique key
 * @param maxDuplicatePercent Maximum allowed percentage of duplicate rows (0.0 to 100.0)
 * @param sampleFraction Optional fraction of data to sample (for large tables)
 */
final case class UniqueCheck(
  tableName: String,
  columns: Seq[String],
  maxDuplicatePercent: Double = 0.0,
  sampleFraction: Option[Double] = None)
  extends DataQualityCheck {

  require(columns.nonEmpty, "columns must not be empty")
  require(
    maxDuplicatePercent >= 0.0 && maxDuplicatePercent <= 100.0,
    "maxDuplicatePercent must be between 0 and 100"
  )
  sampleFraction.foreach(f => require(f > 0.0 && f <= 1.0, "sampleFraction must be between 0 and 1"))

  override val name: String = "unique_check"

  override def run(spark: SparkSession): DataQualityResult = {
    if (!tableExists(spark)) {
      return DataQualityResult.Skipped(name, tableName, s"Table '$tableName' not found")
    }

    val baseDf     = spark.table(tableName)
    val df         = sampleFraction.map(f => baseDf.sample(f)).getOrElse(baseDf)
    val totalCount = df.count()

    if (totalCount == 0) {
      return DataQualityResult.Passed(
        name,
        tableName,
        Map("total_rows" -> 0L, "columns_checked" -> columns.mkString(","))
      )
    }

    val colExprs    = columns.map(col)
    val uniqueCount = df.select(colExprs: _*).distinct().count()
    val dupCount    = totalCount - uniqueCount
    val dupPercent  = (dupCount.toDouble / totalCount) * 100.0

    val details = Map[String, Any](
      "total_rows"           -> totalCount,
      "unique_rows"          -> uniqueCount,
      "duplicate_rows"       -> dupCount,
      "duplicate_percent"    -> f"$dupPercent%.2f",
      "columns_checked"      -> columns.mkString(","),
      "max_duplicate_percent" -> maxDuplicatePercent
    ) ++ sampleFraction.map("sample_fraction" -> _)

    if (dupPercent > maxDuplicatePercent) {
      DataQualityResult.Failed(
        name,
        tableName,
        s"Duplicate percentage ${f"$dupPercent%.2f"}% exceeds maximum $maxDuplicatePercent%",
        details
      )
    } else {
      DataQualityResult.Passed(name, tableName, details)
    }
  }
}

/**
 * Validates that numeric column values fall within a specified range.
 *
 * == Example ==
 *
 * {{{
 * // Ensure age is between 0 and 120
 * RangeCheck("users", column = "age", min = Some(0), max = Some(120))
 *
 * // Ensure price is positive
 * RangeCheck("products", column = "price", min = Some(0.0))
 * }}}
 *
 * @param tableName Name of the table to check
 * @param column Numeric column to check
 * @param min Optional minimum value (inclusive)
 * @param max Optional maximum value (inclusive)
 */
final case class RangeCheck(
  tableName: String,
  column: String,
  min: Option[Double] = None,
  max: Option[Double] = None)
  extends DataQualityCheck {

  require(min.isDefined || max.isDefined, "At least one of min or max must be specified")
  (min, max) match {
    case (Some(minVal), Some(maxVal)) =>
      require(maxVal >= minVal, "max must be >= min")
    case _ => ()
  }

  override val name: String = "range_check"

  override def run(spark: SparkSession): DataQualityResult = {
    if (!tableExists(spark)) {
      return DataQualityResult.Skipped(name, tableName, s"Table '$tableName' not found")
    }

    val df = spark.table(tableName)

    if (!df.columns.contains(column)) {
      return DataQualityResult.Skipped(name, tableName, s"Column '$column' not found in table")
    }

    val totalCount = df.count()
    if (totalCount == 0) {
      return DataQualityResult.Passed(
        name,
        tableName,
        Map("total_rows" -> 0L, "column" -> column)
      )
    }

    val colRef = df(column)

    val outOfRangeFilter = (min, max) match {
      case (Some(minVal), Some(maxVal)) => colRef < minVal || colRef > maxVal
      case (Some(minVal), None)         => colRef < minVal
      case (None, Some(maxVal))         => colRef > maxVal
      case (None, None)                 => lit(false) // Should not happen due to require
    }

    val outOfRangeCount = df.filter(outOfRangeFilter).count()
    val outOfRangePct   = (outOfRangeCount.toDouble / totalCount) * 100.0

    val details = Map[String, Any](
      "total_rows"        -> totalCount,
      "column"            -> column,
      "out_of_range_rows" -> outOfRangeCount,
      "out_of_range_pct"  -> f"$outOfRangePct%.2f"
    ) ++ min.map("min" -> _) ++ max.map("max" -> _)

    if (outOfRangeCount > 0) {
      val rangeDesc = (min, max) match {
        case (Some(minVal), Some(maxVal)) => s"[$minVal, $maxVal]"
        case (Some(minVal), None)         => s">= $minVal"
        case (None, Some(maxVal))         => s"<= $maxVal"
        case _                            => "specified range"
      }
      DataQualityResult.Failed(
        name,
        tableName,
        s"$outOfRangeCount rows (${f"$outOfRangePct%.2f"}%) have '$column' outside range $rangeDesc",
        details
      )
    } else {
      DataQualityResult.Passed(name, tableName, details)
    }
  }
}

/**
 * Executes a custom SQL query and validates the result.
 *
 * The SQL should return a single row with columns that can be validated
 * against expected values.
 *
 * == Example ==
 *
 * {{{
 * // Check that average order value is above threshold
 * CustomSqlCheck(
 *   name = "avg_order_check",
 *   tableName = "orders",
 *   sql = "SELECT AVG(total) as avg_total FROM orders",
 *   expectation = row => row.getAs[Double]("avg_total") > 50.0
 * )
 *
 * // Check row count ratio between tables
 * CustomSqlCheck(
 *   name = "table_ratio_check",
 *   tableName = "events",
 *   sql = "SELECT (SELECT COUNT(*) FROM events) / (SELECT COUNT(*) FROM users) as ratio",
 *   expectation = row => row.getAs[Double]("ratio") >= 1.0
 * )
 * }}}
 *
 * @param checkName Unique name for this check
 * @param tableName Primary table being checked (for reporting)
 * @param sql SQL query to execute
 * @param expectation Function that returns true if the result passes
 * @param failureMessage Optional custom failure message
 */
final case class CustomSqlCheck(
  checkName: String,
  tableName: String,
  sql: String,
  expectation: org.apache.spark.sql.Row => Boolean,
  failureMessage: Option[String] = None)
  extends DataQualityCheck {

  override val name: String = checkName

  override def run(spark: SparkSession): DataQualityResult = {
    try {
      val result = spark.sql(sql).head()
      val passed = expectation(result)

      val details = Map[String, Any](
        "sql"    -> sql,
        "result" -> result.mkString(", ")
      )

      if (passed) {
        DataQualityResult.Passed(name, tableName, details)
      } else {
        DataQualityResult.Failed(
          name,
          tableName,
          failureMessage.getOrElse("Custom SQL check expectation not met"),
          details
        )
      }
    } catch {
      case e: Exception =>
        DataQualityResult.Failed(
          name,
          tableName,
          s"SQL execution failed: ${e.getMessage}",
          Map("sql" -> sql, "error" -> e.getClass.getSimpleName)
        )
    }
  }
}

/**
 * Executes a custom programmatic check with full DataFrame access.
 *
 * Use this when you need complex validation logic that can't be expressed
 * with the built-in checks.
 *
 * == Example ==
 *
 * {{{
 * CustomCheck(
 *   name = "complex_validation",
 *   tableName = "transactions",
 *   validate = (spark, df) => {
 *     val invalid = df.filter($"amount" < 0 && $"type" === "credit")
 *     if (invalid.count() > 0) {
 *       DataQualityResult.Failed("complex_validation", "transactions",
 *         "Found credit transactions with negative amounts")
 *     } else {
 *       DataQualityResult.Passed("complex_validation", "transactions")
 *     }
 *   }
 * )
 * }}}
 *
 * @param checkName Unique name for this check
 * @param tableName Table to validate
 * @param validate Function that performs validation and returns result
 */
final case class CustomCheck(
  checkName: String,
  tableName: String,
  validate: (SparkSession, DataFrame) => DataQualityResult)
  extends DataQualityCheck {

  override val name: String = checkName

  override def run(spark: SparkSession): DataQualityResult = {
    if (!tableExists(spark)) {
      return DataQualityResult.Skipped(name, tableName, s"Table '$tableName' not found")
    }

    try {
      val df = spark.table(tableName)
      validate(spark, df)
    } catch {
      case e: Exception =>
        DataQualityResult.Failed(
          name,
          tableName,
          s"Custom check failed with exception: ${e.getMessage}",
          Map("error" -> e.getClass.getSimpleName)
        )
    }
  }
}
