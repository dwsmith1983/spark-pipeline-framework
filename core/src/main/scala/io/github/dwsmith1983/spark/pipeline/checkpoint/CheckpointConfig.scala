package io.github.dwsmith1983.spark.pipeline.checkpoint

/**
 * Configuration for pipeline checkpointing.
 *
 * Enables pipeline resume functionality by persisting component completion
 * state to a configurable storage location.
 *
 * == HOCON Configuration ==
 *
 * {{{
 * pipeline {
 *   pipeline-name = "My Pipeline"
 *
 *   checkpoint {
 *     enabled = true
 *     location = "/tmp/spark-checkpoints"  # or s3://bucket/path
 *     auto-resume = false
 *     cleanup-on-success = true
 *   }
 *
 *   pipeline-components = [...]
 * }
 * }}}
 *
 * == Configuration Options ==
 *
 *   - '''enabled''': Whether checkpointing is active (default: false)
 *   - '''location''': Directory path for checkpoint storage (default: system temp)
 *   - '''autoResume''': Automatically resume from last checkpoint on startup (default: false)
 *   - '''cleanupOnSuccess''': Delete checkpoint after successful completion (default: true)
 *
 * @param enabled Whether checkpointing is enabled
 * @param location Directory path for checkpoint storage
 * @param autoResume Automatically resume from last failed checkpoint
 * @param cleanupOnSuccess Delete checkpoint files after successful run
 */
case class CheckpointConfig(
  enabled: Boolean = false,
  location: String = FileSystemCheckpointStore.DefaultPath,
  autoResume: Boolean = false,
  cleanupOnSuccess: Boolean = true) {

  /**
   * Creates a checkpoint store based on this configuration.
   *
   * @return CheckpointStore instance configured per settings
   */
  def createStore(): CheckpointStore =
    if (enabled) {
      FileSystemCheckpointStore(location)
    } else {
      CheckpointStore.NoOp
    }

  /**
   * Creates checkpoint hooks based on this configuration.
   *
   * @param runId Optional specific run ID
   * @param resumedFrom Optional run ID being resumed from
   * @return CheckpointHooks if enabled, NoOp PipelineHooks otherwise
   */
  def createHooks(
    runId: Option[String] = None,
    resumedFrom: Option[String] = None
  ): CheckpointHooks = {
    val store = createStore()
    new CheckpointHooks(
      store = store,
      runId = runId.getOrElse(java.util.UUID.randomUUID().toString),
      cleanupOnSuccess = cleanupOnSuccess,
      resumedFrom = resumedFrom
    )
  }
}

object CheckpointConfig {

  /** Default configuration with checkpointing disabled. */
  val default: CheckpointConfig = CheckpointConfig()

  /** Configuration with checkpointing enabled at default location. */
  val enabled: CheckpointConfig = CheckpointConfig(enabled = true)

  /**
   * Creates configuration with checkpointing enabled at specified location.
   *
   * @param location Directory path for checkpoint storage
   * @return CheckpointConfig with checkpointing enabled
   */
  def at(location: String): CheckpointConfig =
    CheckpointConfig(enabled = true, location = location)

  /**
   * Creates configuration with auto-resume enabled.
   *
   * @param location Directory path for checkpoint storage
   * @return CheckpointConfig with auto-resume enabled
   */
  def withAutoResume(location: String): CheckpointConfig =
    CheckpointConfig(enabled = true, location = location, autoResume = true)
}
