package io.github.dwsmith1983.spark.pipeline.checkpoint

import java.time.Instant

/**
 * State model for pipeline checkpointing.
 *
 * Captures the execution state of a pipeline at a point in time, enabling
 * resume functionality for failed pipelines.
 *
 * == Example ==
 *
 * {{{
 * val state = CheckpointState(
 *   runId = "abc123",
 *   pipelineName = "my-etl-pipeline",
 *   startedAt = Instant.now(),
 *   completedComponents = List.empty,
 *   lastCheckpointedIndex = -1,
 *   totalComponents = 5,
 *   status = CheckpointStatus.Running
 * )
 * }}}
 *
 * @param runId Unique identifier for this pipeline run
 * @param pipelineName Name of the pipeline from configuration
 * @param startedAt Timestamp when the pipeline started
 * @param completedComponents List of successfully completed components
 * @param lastCheckpointedIndex Index of last successful component (-1 if none)
 * @param totalComponents Total number of components in the pipeline
 * @param status Current status of the checkpoint (Running, Failed, Completed)
 * @param resumedFrom Optional runId this run was resumed from
 */
case class CheckpointState(
  runId: String,
  pipelineName: String,
  startedAt: Instant,
  completedComponents: List[ComponentCheckpoint],
  lastCheckpointedIndex: Int,
  totalComponents: Int,
  status: CheckpointStatus,
  resumedFrom: Option[String] = None) {

  /**
   * Returns the index to start execution from when resuming.
   * This is lastCheckpointedIndex + 1, or 0 if no components completed.
   */
  def resumeFromIndex: Int = lastCheckpointedIndex + 1

  /** Returns true if this run can be resumed (has incomplete work). */
  def canResume: Boolean = status match {
    case CheckpointStatus.Failed(_, _) => lastCheckpointedIndex < totalComponents - 1
    case _                             => false
  }
}

/**
 * Checkpoint state for a single completed component.
 *
 * @param index Zero-based index of the component in the pipeline
 * @param name Instance name from configuration
 * @param instanceType Fully qualified class name of the component
 * @param startedAt When the component started execution
 * @param completedAt When the component finished execution
 * @param durationMs Execution time in milliseconds
 */
case class ComponentCheckpoint(
  index: Int,
  name: String,
  instanceType: String,
  startedAt: Instant,
  completedAt: Instant,
  durationMs: Long)

/** Status of a checkpoint. */
sealed trait CheckpointStatus

object CheckpointStatus {

  /** Pipeline is currently running. */
  case object Running extends CheckpointStatus

  /**
   * Pipeline failed at a specific component.
   *
   * @param errorMessage Error message from the failure
   * @param failedIndex Index of the component that failed
   */
  case class Failed(errorMessage: String, failedIndex: Int) extends CheckpointStatus

  /** Pipeline completed successfully. */
  case object Completed extends CheckpointStatus
}
