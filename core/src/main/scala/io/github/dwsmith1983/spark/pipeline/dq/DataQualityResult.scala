package io.github.dwsmith1983.spark.pipeline.dq

import java.time.Instant

/**
 * Result of a data quality check execution.
 *
 * Each check produces a result indicating whether the validation passed, failed,
 * produced a warning, or was skipped (e.g., if the target table doesn't exist).
 *
 * == Example ==
 *
 * {{{
 * val result = check.run(spark)
 * result match {
 *   case DataQualityResult.Passed(name, table, details, _) =>
 *     println(s"Check '\$name' passed on '\$table'")
 *   case DataQualityResult.Failed(name, table, message, details, _) =>
 *     println(s"Check '\$name' failed: \$message")
 *   case DataQualityResult.Warning(name, table, message, details, _) =>
 *     println(s"Check '\$name' warning: \$message")
 *   case DataQualityResult.Skipped(name, table, reason, _) =>
 *     println(s"Check '\$name' skipped: \$reason")
 * }
 * }}}
 */
sealed trait DataQualityResult {

  /** Name of the check that produced this result. */
  def checkName: String

  /** Name of the table/view that was checked. */
  def tableName: String

  /** When the check was executed. */
  def timestamp: Instant

  /** Whether this result indicates the check passed. */
  def isPassed: Boolean = this.isInstanceOf[DataQualityResult.Passed]

  /** Whether this result indicates the check failed. */
  def isFailed: Boolean = this.isInstanceOf[DataQualityResult.Failed]

  /** Whether this result is a warning. */
  def isWarning: Boolean = this.isInstanceOf[DataQualityResult.Warning]

  /** Whether this result indicates the check was skipped. */
  def isSkipped: Boolean = this.isInstanceOf[DataQualityResult.Skipped]
}

object DataQualityResult {

  /**
   * Check passed successfully.
   *
   * @param checkName Name of the check
   * @param tableName Name of the table/view checked
   * @param details Additional details about the check (e.g., actual row count)
   * @param timestamp When the check was executed
   */
  final case class Passed(
    checkName: String,
    tableName: String,
    details: Map[String, Any] = Map.empty,
    timestamp: Instant = Instant.now())
    extends DataQualityResult

  /**
   * Check failed - data quality constraint was violated.
   *
   * @param checkName Name of the check
   * @param tableName Name of the table/view checked
   * @param message Description of why the check failed
   * @param details Additional details (e.g., actual vs expected values)
   * @param timestamp When the check was executed
   */
  final case class Failed(
    checkName: String,
    tableName: String,
    message: String,
    details: Map[String, Any] = Map.empty,
    timestamp: Instant = Instant.now())
    extends DataQualityResult

  /**
   * Check produced a warning - soft constraint exceeded but not critical.
   *
   * @param checkName Name of the check
   * @param tableName Name of the table/view checked
   * @param message Description of the warning condition
   * @param details Additional details
   * @param timestamp When the check was executed
   */
  final case class Warning(
    checkName: String,
    tableName: String,
    message: String,
    details: Map[String, Any] = Map.empty,
    timestamp: Instant = Instant.now())
    extends DataQualityResult

  /**
   * Check was skipped and not executed.
   *
   * Common reasons include: table doesn't exist, check is disabled,
   * or preconditions not met.
   *
   * @param checkName Name of the check
   * @param tableName Name of the table/view that was targeted
   * @param reason Why the check was skipped
   * @param timestamp When the skip was determined
   */
  final case class Skipped(
    checkName: String,
    tableName: String,
    reason: String,
    timestamp: Instant = Instant.now())
    extends DataQualityResult
}
