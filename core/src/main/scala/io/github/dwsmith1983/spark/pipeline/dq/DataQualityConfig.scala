package io.github.dwsmith1983.spark.pipeline.dq

/**
 * Defines when a data quality check should be executed during pipeline lifecycle.
 *
 * == Example ==
 *
 * {{{
 * // Run check after a specific component completes
 * CheckTiming.AfterComponent("LoadRawData")
 *
 * // Run check after all components complete
 * CheckTiming.AfterPipeline
 *
 * // Run check before any component starts
 * CheckTiming.BeforePipeline
 * }}}
 */
sealed trait CheckTiming

object CheckTiming {

  /**
   * Execute the check before the pipeline starts (before any component runs).
   * Useful for validating source data or preconditions.
   */
  case object BeforePipeline extends CheckTiming

  /**
   * Execute the check after the entire pipeline completes.
   * Useful for validating final output data.
   */
  case object AfterPipeline extends CheckTiming

  /**
   * Execute the check after a specific component completes.
   *
   * @param componentName The name of the component (instanceName from config)
   */
  final case class AfterComponent(componentName: String) extends CheckTiming
}

/**
 * Defines how the pipeline should behave when data quality checks fail.
 *
 * == Example ==
 *
 * {{{
 * // Stop pipeline on first failure
 * FailureMode.FailOnError
 *
 * // Log failures but continue execution
 * FailureMode.WarnOnly
 *
 * // Fail only if more than 3 checks fail
 * FailureMode.Threshold(3)
 * }}}
 */
sealed trait FailureMode

object FailureMode {

  /**
   * Stop pipeline execution immediately when any check fails.
   * This is the default and most strict mode.
   */
  case object FailOnError extends FailureMode

  /**
   * Log failures as warnings but allow pipeline to continue.
   * Useful for monitoring without blocking production.
   */
  case object WarnOnly extends FailureMode

  /**
   * Allow up to N check failures before stopping the pipeline.
   * Useful when some failures are acceptable.
   *
   * @param maxFailures Maximum number of failures allowed (pipeline fails if exceeded)
   */
  final case class Threshold(maxFailures: Int) extends FailureMode {
    require(maxFailures >= 0, "maxFailures must be non-negative")
  }
}

/**
 * Exception thrown when data quality checks fail and FailureMode requires stopping.
 *
 * @param checkName Name of the check that failed
 * @param tableName Table that was being checked
 * @param message Description of the failure
 * @param failedCount Total number of failed checks
 */
class DataQualityException(
  val checkName: String,
  val tableName: String,
  message: String,
  val failedCount: Int = 1)
  extends RuntimeException(
    s"Data quality check '$checkName' failed on table '$tableName': $message"
  )

object DataQualityException {

  /** Creates exception for a single check failure. */
  def apply(checkName: String, tableName: String, message: String): DataQualityException =
    new DataQualityException(checkName, tableName, message)

  /** Creates exception for multiple check failures. */
  def multiple(failedChecks: Seq[DataQualityResult.Failed]): DataQualityException = {
    val first   = failedChecks.head
    val message = if (failedChecks.size == 1) first.message else s"${failedChecks.size} checks failed"
    new DataQualityException(first.checkName, first.tableName, message, failedChecks.size)
  }
}
