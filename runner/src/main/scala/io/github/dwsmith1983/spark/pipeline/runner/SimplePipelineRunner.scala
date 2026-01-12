package io.github.dwsmith1983.spark.pipeline.runner

import io.github.dwsmith1983.spark.pipeline.checkpoint._
import io.github.dwsmith1983.spark.pipeline.config._
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.{LogManager, Logger}
import pureconfig._
import pureconfig.generic.auto._

/** Exception thrown when schema validation fails between components. */
class SchemaContractViolationException(
  val producerName: String,
  val consumerName: String,
  val errors: List[SchemaValidationError],
  val warnings: List[SchemaValidationWarning])
  extends RuntimeException(
    s"Schema contract violation between '$producerName' and '$consumerName': " +
      errors.map(_.fullMessage).mkString("; ")
  )

/**
 * Main entry point for running pipelines.
 *
 * SimplePipelineRunner reads a HOCON configuration file, configures
 * the SparkSession, and executes pipeline components in sequence.
 *
 * == Usage ==
 *
 * Via spark-submit:
 * {{{
 * spark-submit \
 *   --class io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner \
 *   --jars /path/to/user-pipeline.jar \
 *   /path/to/spark-pipeline-runner.jar \
 *   -Dconfig.file=/path/to/pipeline.conf
 * }}}
 *
 * == Configuration ==
 *
 * The configuration file should contain:
 *
 * {{{
 * spark {
 *   master = "yarn"           # Optional, defaults to spark-submit setting
 *   app-name = "MyPipeline"   # Optional
 *   config {
 *     "spark.executor.memory" = "4g"
 *     "spark.executor.cores" = "2"
 *   }
 * }
 *
 * pipeline {
 *   pipeline-name = "My Data Pipeline"
 *   pipeline-components = [
 *     {
 *       instance-type = "com.mycompany.MyComponent"
 *       instance-name = "MyComponent(prod)"
 *       instance-config {
 *         input-table = "raw_data"
 *         output-path = "/data/processed"
 *       }
 *     }
 *   ]
 * }
 * }}}
 *
 * == Lifecycle Hooks ==
 *
 * You can provide custom `PipelineHooks` to monitor or customize execution:
 *
 * {{{
 * val hooks = new PipelineHooks {
 *   override def afterComponent(config: ComponentConfig, index: Int, total: Int, durationMs: Long): Unit =
 *     metrics.recordDuration(config.instanceName, durationMs)
 * }
 *
 * SimplePipelineRunner.run(config, hooks)
 * }}}
 */
object SimplePipelineRunner extends PipelineRunner {

  private val logger: Logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // Load configuration
    // Config can be specified via:
    // - -Dconfig.file=/path/to/file.conf
    // - -Dconfig.resource=reference.conf (from classpath)
    // - -Dconfig.url=http://...
    val config: Config = ConfigFactory.load()
    run(config)
  }

  /**
   * Runs the pipeline defined in the given configuration with lifecycle hooks.
   *
   * @param config The loaded HOCON configuration
   * @param hooks Lifecycle hooks to invoke during execution
   */
  override def run(config: Config, hooks: PipelineHooks): Unit = {
    val pipelineStartTime: Long                         = System.currentTimeMillis()
    var componentsSucceeded: Int                        = 0
    var pipelineConfig: PipelineConfig                  = null
    var currentComponentConfig: Option[ComponentConfig] = None
    val failures: scala.collection.mutable.ListBuffer[(ComponentConfig, Throwable)] =
      scala.collection.mutable.ListBuffer.empty

    try {
      // Configure SparkSession if spark block is present
      if (config.hasPath("spark")) {
        val sparkConfig: SparkConfig = ConfigSource.fromConfig(config.getConfig("spark")).loadOrThrow[SparkConfig]
        logger.info("Configuring SparkSession from config")
        SparkSessionWrapper.configure(sparkConfig)
      }

      // Load pipeline configuration
      pipelineConfig = ConfigSource.fromConfig(config.getConfig("pipeline")).loadOrThrow[PipelineConfig]

      logger.info(s"Starting pipeline: ${pipelineConfig.pipelineName}")
      logger.info(s"Components to run: ${pipelineConfig.pipelineComponents.size}")
      if (!pipelineConfig.failFast) {
        logger.info("Running in continue-on-error mode (failFast=false)")
      }

      // Get schema validation config (disabled by default)
      val schemaValidationConfig: SchemaValidationConfig =
        pipelineConfig.schemaValidation.getOrElse(SchemaValidationConfig.Default)
      if (schemaValidationConfig.enabled) {
        logger.info(s"Schema validation enabled (strict=${schemaValidationConfig.strict}, failOnWarning=${schemaValidationConfig.failOnWarning})")
      }

      // Invoke beforePipeline hook
      hooks.beforePipeline(pipelineConfig)

      val totalComponents: Int = pipelineConfig.pipelineComponents.size

      // Track previous component for schema validation
      var previousComponent: Option[(PipelineComponent, ComponentConfig)] = None

      // Execute each component in sequence
      pipelineConfig.pipelineComponents.zipWithIndex.foreach {
        case (componentConfig: ComponentConfig, index: Int) =>
          currentComponentConfig = Some(componentConfig)
          val componentNumber: Int = index + 1

          try {
            // Invoke beforeComponent hook
            hooks.beforeComponent(componentConfig, index, totalComponents)

            logger.info(s"[$componentNumber/$totalComponents] Instantiating: ${componentConfig.instanceName}")
            logger.debug(s"Instance type: ${componentConfig.instanceType}")

            val component: PipelineComponent = ComponentInstantiator.instantiate[PipelineComponent](componentConfig)

            // Validate schema contracts between previous and current component
            if (schemaValidationConfig.enabled) {
              validateSchemaContracts(
                previousComponent,
                Some((component, componentConfig)),
                schemaValidationConfig,
                componentNumber,
                totalComponents
              )
            }

            logger.info(s"[$componentNumber/$totalComponents] Running: ${componentConfig.instanceName}")
            val startTime: Long = System.currentTimeMillis()

            component.run()

            val duration: Long = System.currentTimeMillis() - startTime
            logger.info(s"[$componentNumber/$totalComponents] Completed: ${componentConfig.instanceName} (${duration}ms)")

            // Invoke afterComponent hook
            hooks.afterComponent(componentConfig, index, totalComponents, duration)

            componentsSucceeded += 1

            // Update previous component reference for next iteration
            previousComponent = Some((component, componentConfig))
          } catch {
            case e: Exception if !pipelineConfig.failFast =>
              // Continue on error mode: record failure and continue
              logger.error(
                s"[$componentNumber/$totalComponents] Failed: ${componentConfig.instanceName} - ${e.getMessage}",
                e
              )
              hooks.onComponentFailure(componentConfig, index, e)
              failures += ((componentConfig, e))
              // Reset previous component on failure to avoid cascading validation errors
              previousComponent = None
          }
          currentComponentConfig = None
      }

      val totalDuration: Long = System.currentTimeMillis() - pipelineStartTime

      if (failures.isEmpty) {
        logger.info(s"Pipeline completed successfully: ${pipelineConfig.pipelineName}")
        hooks.afterPipeline(pipelineConfig, PipelineResult.Success(totalDuration, componentsSucceeded))
      } else {
        logger.warn(s"Pipeline completed with ${failures.size} failure(s): ${pipelineConfig.pipelineName}")
        val result: PipelineResult = PipelineResult.PartialSuccess(
          totalDuration,
          componentsSucceeded,
          failures.size,
          failures.toList
        )
        hooks.afterPipeline(pipelineConfig, result)
      }

    } catch {
      case e: ComponentInstantiationException =>
        logger.error(s"Failed to instantiate component: ${e.getMessage}", e)
        currentComponentConfig.foreach(cc => hooks.onComponentFailure(cc, componentsSucceeded, e))
        if (pipelineConfig != null) {
          hooks.afterPipeline(pipelineConfig, PipelineResult.Failure(e, currentComponentConfig, componentsSucceeded))
        }
        throw e
      case e: pureconfig.error.ConfigReaderException[_] =>
        val wrapped: ConfigurationException = ConfigurationException.fromConfigReaderException(e)
        logger.error(s"Configuration error: ${wrapped.getMessage}", wrapped)
        if (pipelineConfig != null) {
          hooks.afterPipeline(
            pipelineConfig,
            PipelineResult.Failure(wrapped, currentComponentConfig, componentsSucceeded)
          )
        }
        throw wrapped
      case e: Exception =>
        logger.error(s"Pipeline failed: ${e.getMessage}", e)
        currentComponentConfig.foreach(cc => hooks.onComponentFailure(cc, componentsSucceeded, e))
        if (pipelineConfig != null) {
          hooks.afterPipeline(pipelineConfig, PipelineResult.Failure(e, currentComponentConfig, componentsSucceeded))
        }
        throw e
    }
    // Note: We don't stop SparkSession here as it may be managed externally
    // or reused. The session will be stopped when the JVM exits.
  }

  /**
   * Runs the pipeline from a config file path.
   *
   * @param configPath Path to the HOCON configuration file
   */
  def runFromFile(configPath: String): Unit = {
    val config: Config = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()
    run(config)
  }

  /**
   * Runs the pipeline from a config file path with lifecycle hooks.
   *
   * @param configPath Path to the HOCON configuration file
   * @param hooks Lifecycle hooks to invoke during execution
   */
  def runFromFile(configPath: String, hooks: PipelineHooks): Unit = {
    val config: Config = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()
    run(config, hooks)
  }

  /**
   * Validates the pipeline configuration without executing components.
   *
   * This method parses the configuration and instantiates all components
   * to verify they can be created, but does not call their `run()` methods.
   *
   * @param config The loaded HOCON configuration
   * @param hooks Lifecycle hooks to invoke (only beforePipeline/afterPipeline are called)
   * @return Validation result indicating success or listing errors
   */
  override def dryRun(config: Config, hooks: PipelineHooks): DryRunResult = {
    val errors: scala.collection.mutable.ListBuffer[DryRunError] =
      scala.collection.mutable.ListBuffer.empty

    try {
      // Configure SparkSession if spark block is present
      if (config.hasPath("spark")) {
        val sparkConfig: SparkConfig = ConfigSource.fromConfig(config.getConfig("spark")).loadOrThrow[SparkConfig]
        logger.info("[dry-run] Configuring SparkSession from config")
        SparkSessionWrapper.configure(sparkConfig)
      }

      // Load pipeline configuration
      val pipelineConfig: PipelineConfig =
        ConfigSource.fromConfig(config.getConfig("pipeline")).loadOrThrow[PipelineConfig]

      logger.info(s"[dry-run] Validating pipeline: ${pipelineConfig.pipelineName}")
      logger.info(s"[dry-run] Components to validate: ${pipelineConfig.pipelineComponents.size}")

      // Invoke beforePipeline hook
      hooks.beforePipeline(pipelineConfig)

      val totalComponents: Int = pipelineConfig.pipelineComponents.size

      // Validate each component by instantiating it (but not running)
      pipelineConfig.pipelineComponents.zipWithIndex.foreach {
        case (componentConfig: ComponentConfig, index: Int) =>
          val componentNumber: Int = index + 1
          try {
            logger.info(s"[dry-run] [$componentNumber/$totalComponents] Validating: ${componentConfig.instanceName}")
            logger.debug(s"[dry-run] Instance type: ${componentConfig.instanceType}")

            // Instantiate to validate - this checks class exists, has companion, config is valid
            ComponentInstantiator.instantiate[PipelineComponent](componentConfig)

            logger.info(s"[dry-run] [$componentNumber/$totalComponents] Valid: ${componentConfig.instanceName}")
          } catch {
            case e: Exception =>
              logger.error(s"[dry-run] [$componentNumber/$totalComponents] Invalid: ${componentConfig.instanceName} - ${e.getMessage}")
              errors += DryRunError.ComponentInstantiationError(componentConfig, e)
          }
      }

      if (errors.isEmpty) {
        logger.info(s"[dry-run] Pipeline validation successful: ${pipelineConfig.pipelineName}")
        val result: DryRunResult = DryRunResult.Valid(
          pipelineConfig.pipelineName,
          pipelineConfig.pipelineComponents.size,
          pipelineConfig.pipelineComponents
        )
        hooks.afterPipeline(pipelineConfig, PipelineResult.Success(0L, 0))
        result
      } else {
        logger.error(s"[dry-run] Pipeline validation failed with ${errors.size} error(s)")
        val result: DryRunResult = DryRunResult.Invalid(errors.toList)
        hooks.afterPipeline(
          pipelineConfig,
          PipelineResult.Failure(
            new RuntimeException(s"Dry-run validation failed with ${errors.size} error(s)"),
            None,
            0
          )
        )
        result
      }

    } catch {
      case e: pureconfig.error.ConfigReaderException[_] =>
        val wrapped: ConfigurationException = ConfigurationException.fromConfigReaderException(e)
        logger.error(s"[dry-run] Configuration error: ${wrapped.getMessage}", wrapped)
        DryRunResult.Invalid(List(DryRunError.ConfigParseError(wrapped.getMessage, wrapped)))
      case e: com.typesafe.config.ConfigException =>
        logger.error(s"[dry-run] Configuration error: ${e.getMessage}", e)
        DryRunResult.Invalid(List(DryRunError.ConfigParseError(e.getMessage, e)))
    }
  }

  /**
   * Validates the pipeline from a config file path.
   *
   * @param configPath Path to the HOCON configuration file
   * @return Validation result indicating success or listing errors
   */
  def dryRunFromFile(configPath: String): DryRunResult = {
    val config: Config = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()
    dryRun(config)
  }

  /**
   * Validates the pipeline from a config file path with lifecycle hooks.
   *
   * @param configPath Path to the HOCON configuration file
   * @param hooks Lifecycle hooks to invoke
   * @return Validation result indicating success or listing errors
   */
  def dryRunFromFile(configPath: String, hooks: PipelineHooks): DryRunResult = {
    val config: Config = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()
    dryRun(config, hooks)
  }

  /**
   * Validates the pipeline configuration without instantiating components.
   *
   * This method performs lightweight validation by checking:
   *   - HOCON syntax correctness
   *   - Required fields (pipelineName, pipelineComponents)
   *   - Component class existence (Class.forName)
   *   - Companion object existence and ConfigurableInstance trait
   *
   * Unlike `dryRun`, this does NOT call `createFromConfig` on components,
   * making it faster and free of initialization side effects.
   *
   * Use `validate` for quick pre-flight checks, and `dryRun` when you need
   * to verify that components can be fully instantiated with their configs.
   *
   * @param config The loaded HOCON configuration
   * @return Validation result with all errors and warnings
   */
  def validate(config: Config): ValidationResult = {
    logger.info("[validate] Starting configuration validation")
    val result = ConfigValidator.validate(config)
    result match {
      case ValidationResult.Valid(name, count, warnings) =>
        logger.info(s"[validate] Configuration valid: $name ($count components)")
        warnings.foreach(w => logger.warn(s"[validate] Warning: ${w.fullMessage}"))
      case ValidationResult.Invalid(errors, warnings) =>
        logger.error(s"[validate] Configuration invalid: ${errors.size} error(s)")
        errors.foreach(e => logger.error(s"[validate] ${e.fullMessage}"))
        warnings.foreach(w => logger.warn(s"[validate] Warning: ${w.fullMessage}"))
    }
    result
  }

  /**
   * Validates the pipeline configuration with options.
   *
   * @param config  The loaded HOCON configuration
   * @param options Validation options (e.g., resource checking)
   * @return Validation result with all errors and warnings
   */
  def validate(config: Config, options: ValidationOptions): ValidationResult = {
    logger.info("[validate] Starting configuration validation")
    val result = ConfigValidator.validate(config, options)
    result match {
      case ValidationResult.Valid(name, count, warnings) =>
        logger.info(s"[validate] Configuration valid: $name ($count components)")
        warnings.foreach(w => logger.warn(s"[validate] Warning: ${w.fullMessage}"))
      case ValidationResult.Invalid(errors, warnings) =>
        logger.error(s"[validate] Configuration invalid: ${errors.size} error(s)")
        errors.foreach(e => logger.error(s"[validate] ${e.fullMessage}"))
        warnings.foreach(w => logger.warn(s"[validate] Warning: ${w.fullMessage}"))
    }
    result
  }

  /**
   * Validates a pipeline configuration from a file path.
   *
   * @param configPath Path to the HOCON configuration file
   * @return Validation result with all errors and warnings
   */
  def validateFromFile(configPath: String): ValidationResult = {
    logger.info(s"[validate] Validating configuration file: $configPath")
    ConfigValidator.validateFromFile(configPath)
  }

  /**
   * Validates a pipeline configuration from a file path with options.
   *
   * @param configPath Path to the HOCON configuration file
   * @param options    Validation options (e.g., resource checking)
   * @return Validation result with all errors and warnings
   */
  def validateFromFile(configPath: String, options: ValidationOptions): ValidationResult = {
    logger.info(s"[validate] Validating configuration file: $configPath")
    ConfigValidator.validateFromFile(configPath, options)
  }

  /**
   * Validates schema contracts between adjacent components.
   *
   * @param previous         The previous component (producer), if any
   * @param current          The current component (consumer), if any
   * @param config           Schema validation configuration
   * @param componentNumber  Current component number (for logging)
   * @param totalComponents  Total number of components (for logging)
   * @throws SchemaContractViolationException if validation fails
   */
  private def validateSchemaContracts(
    previous: Option[(PipelineComponent, ComponentConfig)],
    current: Option[(PipelineComponent, ComponentConfig)],
    config: SchemaValidationConfig,
    componentNumber: Int,
    totalComponents: Int
  ): Unit = {
    // Extract SchemaContract trait if present
    val producerContract: Option[SchemaContract] = previous.flatMap {
      case (comp, _) => comp match {
          case sc: SchemaContract => Some(sc)
          case _                  => None
        }
    }
    val consumerContract: Option[SchemaContract] = current.flatMap {
      case (comp, _) => comp match {
          case sc: SchemaContract => Some(sc)
          case _                  => None
        }
    }

    val result: SchemaValidationResult =
      SchemaValidator.validateComponents(producerContract, consumerContract, config)

    result match {
      case SchemaValidationResult.Skipped =>
        // No validation needed
        ()

      case SchemaValidationResult.Valid(warnings) =>
        // Log warnings
        warnings.foreach { w =>
          val producerName = previous.map(_._2.instanceName).getOrElse("(start)")
          val consumerName = current.map(_._2.instanceName).getOrElse("(end)")
          logger.warn(
            s"[$componentNumber/$totalComponents] Schema warning between " +
              s"'$producerName' and '$consumerName': ${w.fullMessage}"
          )
        }
        // Fail on warning if configured
        if (config.failOnWarning && warnings.nonEmpty) {
          val producerName = previous.map(_._2.instanceName).getOrElse("(start)")
          val consumerName = current.map(_._2.instanceName).getOrElse("(end)")
          throw new SchemaContractViolationException(
            producerName,
            consumerName,
            warnings.map(w =>
              SchemaValidationError(
                SchemaErrorType.Incompatible,
                w.fullMessage,
                w.fieldName
              )
            ),
            warnings
          )
        }

      case SchemaValidationResult.Invalid(errors, warnings) =>
        val producerName = previous.map(_._2.instanceName).getOrElse("(start)")
        val consumerName = current.map(_._2.instanceName).getOrElse("(end)")

        // Log all errors and warnings
        errors.foreach { e =>
          logger.error(
            s"[$componentNumber/$totalComponents] Schema error between " +
              s"'$producerName' and '$consumerName': ${e.fullMessage}"
          )
        }
        warnings.foreach { w =>
          logger.warn(
            s"[$componentNumber/$totalComponents] Schema warning between " +
              s"'$producerName' and '$consumerName': ${w.fullMessage}"
          )
        }

        throw new SchemaContractViolationException(producerName, consumerName, errors, warnings)
    }
  }

  // ============================================================================
  // Checkpoint and Resume Support
  // ============================================================================

  /**
   * Resumes a failed pipeline run from its checkpoint.
   *
   * Loads the checkpoint state for the specified run ID and continues
   * execution from the next component after the last successful one.
   *
   * == Example ==
   *
   * {{{
   * // Resume from a specific failed run
   * SimplePipelineRunner.resume(
   *   configPath = "/path/to/pipeline.conf",
   *   runId = "abc123-def456"
   * )
   * }}}
   *
   * @param configPath Path to the HOCON configuration file
   * @param runId The run ID to resume from
   * @param store Optional checkpoint store (uses config-based store if not provided)
   * @param additionalHooks Additional hooks to compose with checkpoint hooks
   * @note Throws `CheckpointException` if checkpoint not found or config mismatch
   */
  def resume(
    configPath: String,
    runId: String,
    store: Option[CheckpointStore] = None,
    additionalHooks: PipelineHooks = PipelineHooks.NoOp
  ): Unit = {
    val config: Config = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()
    resumeFromConfig(config, runId, store, additionalHooks)
  }

  /**
   * Resumes a failed pipeline run using a Config object.
   *
   * @param config The loaded HOCON configuration
   * @param runId The run ID to resume from
   * @param store Optional checkpoint store
   * @param additionalHooks Additional hooks to compose with checkpoint hooks
   */
  def resumeFromConfig(
    config: Config,
    runId: String,
    store: Option[CheckpointStore] = None,
    additionalHooks: PipelineHooks = PipelineHooks.NoOp
  ): Unit = {
    // Load pipeline configuration
    val pipelineConfig: PipelineConfig =
      ConfigSource.fromConfig(config.getConfig("pipeline")).loadOrThrow[PipelineConfig]

    // Get checkpoint store from config or parameter
    val checkpointStore: CheckpointStore = store.getOrElse {
      pipelineConfig.checkpoint
        .filter(_.enabled)
        .map(_.createStore())
        .getOrElse(throw new CheckpointException(
          "Cannot resume: no checkpoint store provided and checkpointing not enabled in config"
        ))
    }

    // Load the checkpoint
    val checkpoint: CheckpointState = checkpointStore.loadByRunId(runId).getOrElse {
      throw new CheckpointException(s"Checkpoint not found for run: $runId")
    }

    // Validate configuration matches
    if (checkpoint.totalComponents != pipelineConfig.pipelineComponents.size) {
      throw CheckpointException.configMismatch(
        checkpoint.totalComponents,
        pipelineConfig.pipelineComponents.size
      )
    }

    val startIndex: Int = checkpoint.resumeFromIndex
    logger.info(s"Resuming pipeline from component ${startIndex + 1}/${checkpoint.totalComponents}")
    logger.info(s"Original run: $runId, ${checkpoint.completedComponents.size} components already completed")

    // Create checkpoint hooks for the resumed run
    val checkpointHooks: CheckpointHooks = CheckpointHooks.forResume(checkpointStore, runId)

    // Combine with additional hooks
    val combinedHooks: PipelineHooks = PipelineHooks.compose(checkpointHooks, additionalHooks)

    // Run from the resume index
    runFromIndex(config, combinedHooks, startIndex)
  }

  /**
   * Runs a pipeline with automatic checkpoint support.
   *
   * If checkpointing is enabled in the configuration and auto-resume is true,
   * this method will automatically resume from the last failed checkpoint if one exists.
   *
   * == Example ==
   *
   * {{{
   * // Run with checkpointing enabled (configured in pipeline.conf)
   * SimplePipelineRunner.runWithCheckpointing("/path/to/pipeline.conf")
   * }}}
   *
   * @param configPath Path to the HOCON configuration file
   * @param additionalHooks Additional hooks to compose with checkpoint hooks
   */
  def runWithCheckpointing(
    configPath: String,
    additionalHooks: PipelineHooks = PipelineHooks.NoOp
  ): Unit = {
    val config: Config = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()
    runWithCheckpointingFromConfig(config, additionalHooks)
  }

  /**
   * Runs a pipeline with automatic checkpoint support using a Config object.
   *
   * @param config The loaded HOCON configuration
   * @param additionalHooks Additional hooks to compose with checkpoint hooks
   */
  def runWithCheckpointingFromConfig(
    config: Config,
    additionalHooks: PipelineHooks = PipelineHooks.NoOp
  ): Unit = {
    val pipelineConfig: PipelineConfig =
      ConfigSource.fromConfig(config.getConfig("pipeline")).loadOrThrow[PipelineConfig]

    val checkpointConfig: Option[CheckpointConfig] = pipelineConfig.checkpoint.filter(_.enabled)

    checkpointConfig match {
      case Some(cpConfig) if cpConfig.autoResume =>
        // Check for existing checkpoint to resume
        val store: CheckpointStore = cpConfig.createStore()
        store.loadLatest(pipelineConfig.pipelineName) match {
          case Some(checkpoint) if checkpoint.canResume =>
            logger.info(s"Auto-resuming from checkpoint: ${checkpoint.runId}")
            resumeFromConfig(config, checkpoint.runId, Some(store), additionalHooks)
          case _ =>
            // No checkpoint to resume, run fresh with checkpointing
            runFreshWithCheckpointing(config, cpConfig, additionalHooks)
        }

      case Some(cpConfig) =>
        // Checkpointing enabled but not auto-resume
        runFreshWithCheckpointing(config, cpConfig, additionalHooks)

      case None =>
        // No checkpointing configured, run normally
        run(config, additionalHooks)
    }
  }

  /**
   * Lists all incomplete (resumable) checkpoints for a pipeline.
   *
   * @param pipelineName Name of the pipeline
   * @param store The checkpoint store to query
   * @return List of checkpoint states that can be resumed
   */
  def listResumableCheckpoints(
    pipelineName: String,
    store: CheckpointStore
  ): List[CheckpointState] =
    store.listIncomplete(pipelineName)

  // ============================================================================
  // Private Implementation Methods
  // ============================================================================

  private def runFreshWithCheckpointing(
    config: Config,
    checkpointConfig: CheckpointConfig,
    additionalHooks: PipelineHooks
  ): Unit = {
    val checkpointHooks: CheckpointHooks = checkpointConfig.createHooks()
    val combinedHooks: PipelineHooks     = PipelineHooks.compose(checkpointHooks, additionalHooks)
    run(config, combinedHooks)
  }

  /**
   * Runs the pipeline starting from a specific component index.
   *
   * Components before startIndex are skipped. This is used internally
   * for resume functionality.
   *
   * @param config The loaded HOCON configuration
   * @param hooks Lifecycle hooks to invoke during execution
   * @param startIndex Zero-based index of the first component to execute
   */
  private def runFromIndex(config: Config, hooks: PipelineHooks, startIndex: Int): Unit = {
    val pipelineStartTime: Long                         = System.currentTimeMillis()
    var componentsSucceeded: Int                        = startIndex // Count skipped as "succeeded"
    var pipelineConfig: PipelineConfig                  = null
    var currentComponentConfig: Option[ComponentConfig] = None
    val failures: scala.collection.mutable.ListBuffer[(ComponentConfig, Throwable)] =
      scala.collection.mutable.ListBuffer.empty

    try {
      // Configure SparkSession if spark block is present
      if (config.hasPath("spark")) {
        val sparkConfig: SparkConfig = ConfigSource.fromConfig(config.getConfig("spark")).loadOrThrow[SparkConfig]
        logger.info("Configuring SparkSession from config")
        SparkSessionWrapper.configure(sparkConfig)
      }

      // Load pipeline configuration
      pipelineConfig = ConfigSource.fromConfig(config.getConfig("pipeline")).loadOrThrow[PipelineConfig]

      logger.info(s"Starting pipeline: ${pipelineConfig.pipelineName}")
      logger.info(s"Resuming from component ${startIndex + 1}/${pipelineConfig.pipelineComponents.size}")
      if (!pipelineConfig.failFast) {
        logger.info("Running in continue-on-error mode (failFast=false)")
      }

      // Invoke beforePipeline hook
      hooks.beforePipeline(pipelineConfig)

      val totalComponents: Int = pipelineConfig.pipelineComponents.size

      // Execute each component starting from startIndex
      pipelineConfig.pipelineComponents.zipWithIndex.drop(startIndex).foreach {
        case (componentConfig: ComponentConfig, index: Int) =>
          currentComponentConfig = Some(componentConfig)
          val componentNumber: Int = index + 1

          try {
            // Invoke beforeComponent hook
            hooks.beforeComponent(componentConfig, index, totalComponents)

            logger.info(s"[$componentNumber/$totalComponents] Instantiating: ${componentConfig.instanceName}")
            logger.debug(s"Instance type: ${componentConfig.instanceType}")

            val component: PipelineComponent = ComponentInstantiator.instantiate[PipelineComponent](componentConfig)

            logger.info(s"[$componentNumber/$totalComponents] Running: ${componentConfig.instanceName}")
            val componentStartTime: Long = System.currentTimeMillis()

            component.run()

            val duration: Long = System.currentTimeMillis() - componentStartTime
            logger.info(s"[$componentNumber/$totalComponents] Completed: ${componentConfig.instanceName} (${duration}ms)")

            // Invoke afterComponent hook
            hooks.afterComponent(componentConfig, index, totalComponents, duration)

            componentsSucceeded += 1
          } catch {
            case e: Exception if !pipelineConfig.failFast =>
              // Continue on error mode: record failure and continue
              logger.error(
                s"[$componentNumber/$totalComponents] Failed: ${componentConfig.instanceName} - ${e.getMessage}",
                e
              )
              hooks.onComponentFailure(componentConfig, index, e)
              failures += ((componentConfig, e))
          }
          currentComponentConfig = None
      }

      val totalDuration: Long = System.currentTimeMillis() - pipelineStartTime

      if (failures.isEmpty) {
        logger.info(s"Pipeline completed successfully: ${pipelineConfig.pipelineName}")
        hooks.afterPipeline(pipelineConfig, PipelineResult.Success(totalDuration, componentsSucceeded))
      } else {
        logger.warn(s"Pipeline completed with ${failures.size} failure(s): ${pipelineConfig.pipelineName}")
        val result: PipelineResult = PipelineResult.PartialSuccess(
          totalDuration,
          componentsSucceeded,
          failures.size,
          failures.toList
        )
        hooks.afterPipeline(pipelineConfig, result)
      }

    } catch {
      case e: ComponentInstantiationException =>
        logger.error(s"Failed to instantiate component: ${e.getMessage}", e)
        currentComponentConfig.foreach(cc => hooks.onComponentFailure(cc, componentsSucceeded, e))
        if (pipelineConfig != null) {
          hooks.afterPipeline(pipelineConfig, PipelineResult.Failure(e, currentComponentConfig, componentsSucceeded))
        }
        throw e
      case e: pureconfig.error.ConfigReaderException[_] =>
        val wrapped: ConfigurationException = ConfigurationException.fromConfigReaderException(e)
        logger.error(s"Configuration error: ${wrapped.getMessage}", wrapped)
        if (pipelineConfig != null) {
          hooks.afterPipeline(
            pipelineConfig,
            PipelineResult.Failure(wrapped, currentComponentConfig, componentsSucceeded)
          )
        }
        throw wrapped
      case e: Exception =>
        logger.error(s"Pipeline failed: ${e.getMessage}", e)
        currentComponentConfig.foreach(cc => hooks.onComponentFailure(cc, componentsSucceeded, e))
        if (pipelineConfig != null) {
          hooks.afterPipeline(pipelineConfig, PipelineResult.Failure(e, currentComponentConfig, componentsSucceeded))
        }
        throw e
    }
  }
}
