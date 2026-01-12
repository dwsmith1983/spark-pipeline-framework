package io.github.dwsmith1983.spark.pipeline.checkpoint

import io.github.dwsmith1983.spark.pipeline.config._
import org.apache.logging.log4j.{LogManager, Logger}

import java.time.Instant
import scala.collection.mutable

/**
 * Pipeline lifecycle hooks for checkpointing.
 *
 * Records component completion state to enable pipeline resume functionality.
 * After each successful component, the checkpoint state is persisted to the
 * configured store.
 *
 * == Basic Usage ==
 *
 * {{{
 * val store = FileSystemCheckpointStore("/tmp/checkpoints")
 * val hooks = CheckpointHooks(store)
 *
 * SimplePipelineRunner.run(config, hooks)
 * }}}
 *
 * == With Other Hooks ==
 *
 * {{{
 * val checkpointHooks = CheckpointHooks(store)
 * val loggingHooks = LoggingHooks.structured()
 *
 * val combined = PipelineHooks.compose(checkpointHooks, loggingHooks)
 * SimplePipelineRunner.run(config, combined)
 * }}}
 *
 * @param store The checkpoint store for persistence
 * @param runId Unique identifier for this run (auto-generated if not provided)
 * @param cleanupOnSuccess Whether to delete checkpoint after successful completion
 * @param resumedFrom Optional runId this run was resumed from
 */
class CheckpointHooks(
  val store: CheckpointStore,
  val runId: String = java.util.UUID.randomUUID().toString,
  val cleanupOnSuccess: Boolean = true,
  val resumedFrom: Option[String] = None)
  extends PipelineHooks {

  private val logger: Logger = LogManager.getLogger(getClass)

  @volatile private var currentState: Option[CheckpointState] = None
  @volatile private var pipelineStartTime: Instant            = _
  private val componentStartTimes: mutable.Map[Int, Instant]  = mutable.Map()

  override def beforePipeline(config: PipelineConfig): Unit = {
    pipelineStartTime = Instant.now()

    currentState = Some(
      CheckpointState(
        runId = runId,
        pipelineName = config.pipelineName,
        startedAt = pipelineStartTime,
        completedComponents = List.empty,
        lastCheckpointedIndex = -1,
        totalComponents = config.pipelineComponents.size,
        status = CheckpointStatus.Running,
        resumedFrom = resumedFrom
      )
    )

    try {
      store.save(currentState.get)
      logger.info(s"Checkpoint initialized for run: $runId")
    } catch {
      case e: CheckpointException =>
        logger.warn(s"Failed to save initial checkpoint: ${e.getMessage}")
    }
  }

  override def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit =
    componentStartTimes(index) = Instant.now()

  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    val startTime   = componentStartTimes.getOrElse(index, Instant.now().minusMillis(durationMs))
    val completedAt = Instant.now()
    locally { val _ = componentStartTimes.remove(index) }

    val checkpoint = ComponentCheckpoint(
      index = index,
      name = config.instanceName,
      instanceType = config.instanceType,
      startedAt = startTime,
      completedAt = completedAt,
      durationMs = durationMs
    )

    currentState = currentState.map { state =>
      state.copy(
        completedComponents = state.completedComponents :+ checkpoint,
        lastCheckpointedIndex = index
      )
    }

    currentState.foreach { state =>
      try {
        store.save(state)
        logger.debug(s"Checkpoint updated: component ${index + 1}/$total completed")
      } catch {
        case e: CheckpointException =>
          logger.warn(s"Failed to save checkpoint after component $index: ${e.getMessage}")
      }
    }
  }

  override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {
    locally { val _ = componentStartTimes.remove(index) }

    val errorMessage = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)

    currentState = currentState.map(state => state.copy(status = CheckpointStatus.Failed(errorMessage, index)))

    currentState.foreach { state =>
      try {
        store.save(state)
        logger.info(s"Checkpoint saved after failure at component $index: $errorMessage")
      } catch {
        case e: CheckpointException =>
          logger.warn(s"Failed to save checkpoint on failure: ${e.getMessage}")
      }
    }
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit =
    result match {
      case PipelineResult.Success(_, _) =>
        currentState = currentState.map(_.copy(status = CheckpointStatus.Completed))
        currentState.foreach { state =>
          if (cleanupOnSuccess) {
            try {
              store.delete(runId)
              logger.info(s"Checkpoint cleaned up after successful run: $runId")
            } catch {
              case e: CheckpointException =>
                logger.warn(s"Failed to delete checkpoint: ${e.getMessage}")
            }
          } else {
            try {
              store.save(state)
            } catch {
              case e: CheckpointException =>
                logger.warn(s"Failed to save final checkpoint: ${e.getMessage}")
            }
          }
        }

      case PipelineResult.Failure(error, failedComponent, _) =>
        // State should already be updated by onComponentFailure
        // But ensure it's marked as failed
        val errorMsg = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
        val failedIdx = failedComponent
          .flatMap(cc => config.pipelineComponents.zipWithIndex.find(_._1 == cc).map(_._2))
          .getOrElse(-1)

        currentState = currentState.map { state =>
          if (state.status == CheckpointStatus.Running) {
            state.copy(status = CheckpointStatus.Failed(errorMsg, failedIdx))
          } else {
            state
          }
        }

        currentState.foreach { state =>
          try {
            store.save(state)
            logger.info(s"Checkpoint finalized for failed run: $runId")
          } catch {
            case e: CheckpointException =>
              logger.warn(s"Failed to save final checkpoint: ${e.getMessage}")
          }
        }

      case PipelineResult.PartialSuccess(_, _, _, failures) =>
        // Mark as failed with first failure info
        val (failedConfig, error) = failures.head
        val errorMsg              = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
        val failedIdx             = config.pipelineComponents.indexOf(failedConfig)

        currentState = currentState.map(state => state.copy(status = CheckpointStatus.Failed(errorMsg, failedIdx)))

        currentState.foreach { state =>
          try {
            store.save(state)
            logger.info(s"Checkpoint finalized for partial success: $runId")
          } catch {
            case e: CheckpointException =>
              logger.warn(s"Failed to save final checkpoint: ${e.getMessage}")
          }
        }
    }

  /** Returns the current checkpoint state. */
  def getState: Option[CheckpointState] = currentState
}

object CheckpointHooks {

  /**
   * Creates checkpoint hooks with a store.
   *
   * @param store The checkpoint store
   * @return New CheckpointHooks instance
   */
  def apply(store: CheckpointStore): CheckpointHooks =
    new CheckpointHooks(store)

  /**
   * Creates checkpoint hooks with full configuration.
   *
   * @param store The checkpoint store
   * @param runId Unique run identifier
   * @param cleanupOnSuccess Whether to delete checkpoint on success
   * @param resumedFrom Optional runId being resumed from
   * @return New CheckpointHooks instance
   */
  def apply(
    store: CheckpointStore,
    runId: String = java.util.UUID.randomUUID().toString,
    cleanupOnSuccess: Boolean = true,
    resumedFrom: Option[String] = None
  ): CheckpointHooks =
    new CheckpointHooks(store, runId, cleanupOnSuccess, resumedFrom)

  /**
   * Creates checkpoint hooks for resuming a failed run.
   *
   * @param store The checkpoint store
   * @param originalRunId The runId of the failed run being resumed
   * @return New CheckpointHooks configured for resume
   */
  def forResume(store: CheckpointStore, originalRunId: String): CheckpointHooks =
    new CheckpointHooks(
      store = store,
      runId = java.util.UUID.randomUUID().toString,
      cleanupOnSuccess = true,
      resumedFrom = Some(originalRunId)
    )

  /**
   * Creates checkpoint hooks using the default file system store.
   *
   * @return New CheckpointHooks with default FileSystemCheckpointStore
   */
  def withDefaultStore(): CheckpointHooks =
    new CheckpointHooks(FileSystemCheckpointStore.default())
}
