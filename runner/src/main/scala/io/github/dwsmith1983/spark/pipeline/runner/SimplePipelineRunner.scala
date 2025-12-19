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
    var componentsCompleted: Int                        = 0
    var pipelineConfig: PipelineConfig                  = null
    var currentComponentConfig: Option[ComponentConfig] = None

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

      // Invoke beforePipeline hook
      hooks.beforePipeline(pipelineConfig)

      val totalComponents: Int = pipelineConfig.pipelineComponents.size

      // Execute each component in sequence
      pipelineConfig.pipelineComponents.zipWithIndex.foreach {
        case (componentConfig: ComponentConfig, index: Int) =>
          currentComponentConfig = Some(componentConfig)
          val componentNumber: Int = index + 1

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

          componentsCompleted += 1
          currentComponentConfig = None
      }

      logger.info(s"Pipeline completed successfully: ${pipelineConfig.pipelineName}")

      // Invoke afterPipeline hook with success result
      val totalDuration: Long = System.currentTimeMillis() - pipelineStartTime
      hooks.afterPipeline(pipelineConfig, PipelineResult.Success(totalDuration, componentsCompleted))

    } catch {
      case e: ComponentInstantiationException =>
        logger.error(s"Failed to instantiate component: ${e.getMessage}", e)
        currentComponentConfig.foreach(cc => hooks.onComponentFailure(cc, componentsCompleted, e))
        if (pipelineConfig != null) {
          hooks.afterPipeline(pipelineConfig, PipelineResult.Failure(e, currentComponentConfig, componentsCompleted))
        }
        throw e
      case e: pureconfig.error.ConfigReaderException[_] =>
        logger.error(s"Configuration error: ${e.getMessage}", e)
        if (pipelineConfig != null) {
          hooks.afterPipeline(pipelineConfig, PipelineResult.Failure(e, currentComponentConfig, componentsCompleted))
        }
        throw e
      case e: Exception =>
        logger.error(s"Pipeline failed: ${e.getMessage}", e)
        currentComponentConfig.foreach(cc => hooks.onComponentFailure(cc, componentsCompleted, e))
        if (pipelineConfig != null) {
          hooks.afterPipeline(pipelineConfig, PipelineResult.Failure(e, currentComponentConfig, componentsCompleted))
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
}
