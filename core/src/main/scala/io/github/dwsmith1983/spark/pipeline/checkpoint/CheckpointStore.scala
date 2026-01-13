package io.github.dwsmith1983.spark.pipeline.checkpoint

/**
 * Storage interface for pipeline checkpoints.
 *
 * Implementations provide persistence for checkpoint state, enabling
 * pipeline resume functionality across different storage backends.
 *
 * == Thread Safety ==
 *
 * Implementations should be thread-safe for concurrent reads but may
 * assume single-writer semantics (only one pipeline run writes to a
 * given checkpoint at a time).
 *
 * == Example ==
 *
 * {{{
 * val store = FileSystemCheckpointStore("/tmp/checkpoints")
 *
 * // Save checkpoint
 * store.save(checkpointState)
 *
 * // Load latest checkpoint for resume
 * val checkpoint = store.loadLatest("my-pipeline")
 *
 * // Clean up after successful completion
 * store.delete("run-123")
 * }}}
 */
trait CheckpointStore {

  /**
   * Saves checkpoint state atomically.
   *
   * If a checkpoint with the same runId exists, it will be overwritten.
   *
   * @param state The checkpoint state to save
   * @throws CheckpointException if the save operation fails
   */
  def save(state: CheckpointState): Unit

  /**
   * Loads the most recent incomplete checkpoint for a pipeline.
   *
   * Returns the checkpoint with the most recent startedAt timestamp
   * that has a Failed status.
   *
   * @param pipelineName Name of the pipeline
   * @return The most recent incomplete checkpoint, or None if not found
   */
  def loadLatest(pipelineName: String): Option[CheckpointState]

  /**
   * Loads a checkpoint by its specific run ID.
   *
   * @param runId The run ID to load
   * @return The checkpoint state, or None if not found
   */
  def loadByRunId(runId: String): Option[CheckpointState]

  /**
   * Deletes a checkpoint by run ID.
   *
   * Used to clean up after successful pipeline completion.
   *
   * @param runId The run ID to delete
   */
  def delete(runId: String): Unit

  /**
   * Lists all incomplete (failed) checkpoints.
   *
   * @return List of all checkpoints with Failed status
   */
  def listIncomplete(): List[CheckpointState]

  /**
   * Lists all incomplete checkpoints for a specific pipeline.
   *
   * @param pipelineName Name of the pipeline
   * @return List of incomplete checkpoints for the pipeline
   */
  def listIncomplete(pipelineName: String): List[CheckpointState]
}

object CheckpointStore {

  /**
   * A no-op checkpoint store that doesn't persist anything.
   *
   * Useful for testing or when checkpointing is disabled.
   */
  val NoOp: CheckpointStore = new CheckpointStore {
    override def save(state: CheckpointState): Unit                          = ()
    override def loadLatest(pipelineName: String): Option[CheckpointState]   = None
    override def loadByRunId(runId: String): Option[CheckpointState]         = None
    override def delete(runId: String): Unit                                 = ()
    override def listIncomplete(): List[CheckpointState]                     = List.empty
    override def listIncomplete(pipelineName: String): List[CheckpointState] = List.empty
  }
}

/**
 * Exception thrown when checkpoint operations fail.
 *
 * @param message Description of the failure
 * @param cause Underlying cause, if any
 */
class CheckpointException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object CheckpointException {

  def saveFailure(runId: String, cause: Throwable): CheckpointException =
    new CheckpointException(s"Failed to save checkpoint for run: $runId", cause)

  def loadFailure(runId: String, cause: Throwable): CheckpointException =
    new CheckpointException(s"Failed to load checkpoint for run: $runId", cause)

  def deleteFailure(runId: String, cause: Throwable): CheckpointException =
    new CheckpointException(s"Failed to delete checkpoint for run: $runId", cause)

  def corruptedCheckpoint(runId: String, cause: Throwable): CheckpointException =
    new CheckpointException(s"Checkpoint data corrupted for run: $runId", cause)

  def configMismatch(expectedComponents: Int, actualComponents: Int): CheckpointException =
    new CheckpointException(
      s"Pipeline configuration mismatch: checkpoint has $expectedComponents components, " +
        s"current config has $actualComponents components. Cannot safely resume."
    )
}
