package io.github.dwsmith1983.spark.pipeline.runner

import io.github.dwsmith1983.spark.pipeline.config._
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.{LogManager, Logger}
import pureconfig._
import pureconfig.generic.auto._

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

      // Invoke beforePipeline hook
      hooks.beforePipeline(pipelineConfig)

      val totalComponents: Int = pipelineConfig.pipelineComponents.size

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

            logger.info(s"[$componentNumber/$totalComponents] Running: ${componentConfig.instanceName}")
            val startTime: Long = System.currentTimeMillis()

            component.run()

            val duration: Long = System.currentTimeMillis() - startTime
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
}
