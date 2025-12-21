package io.github.dwsmith1983.spark.pipeline.config

/**
 * Result of a pipeline execution.
 *
 * Used by [[PipelineHooks]] to communicate pipeline completion status.
 */
sealed trait PipelineResult {

  /** Whether the pipeline completed successfully. */
  def isSuccess: Boolean

  /** Whether the pipeline failed. */
  def isFailure: Boolean = !isSuccess
}

object PipelineResult {

  /**
   * Indicates successful pipeline completion.
   *
   * @param durationMs Total pipeline execution time in milliseconds
   * @param componentsRun Number of components that were executed
   */
  final case class Success(durationMs: Long, componentsRun: Int) extends PipelineResult {
    override def isSuccess: Boolean = true
  }

  /**
   * Indicates pipeline failure.
   *
   * @param error The exception that caused the failure
   * @param failedComponent The component configuration that failed, if applicable
   * @param componentsCompleted Number of components that completed before failure
   */
  final case class Failure(
    error: Throwable,
    failedComponent: Option[ComponentConfig],
    componentsCompleted: Int)
    extends PipelineResult {
    override def isSuccess: Boolean = false
  }

  /**
   * Indicates partial pipeline completion when `failFast = false`.
   *
   * Some components succeeded while others failed. The pipeline continued
   * execution after failures instead of stopping at the first error.
   *
   * @param durationMs Total pipeline execution time in milliseconds
   * @param componentsSucceeded Number of components that completed successfully
   * @param componentsFailed Number of components that failed
   * @param failures List of failed components with their errors
   */
  final case class PartialSuccess(
    durationMs: Long,
    componentsSucceeded: Int,
    componentsFailed: Int,
    failures: List[(ComponentConfig, Throwable)])
    extends PipelineResult {
    override def isSuccess: Boolean = false
  }
}
