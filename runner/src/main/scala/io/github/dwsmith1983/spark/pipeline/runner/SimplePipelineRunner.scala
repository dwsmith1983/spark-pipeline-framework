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
 */
object SimplePipelineRunner {

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
   * Runs the pipeline defined in the given configuration.
   *
   * @param config The loaded HOCON configuration
   */
  def run(config: Config): Unit =
    try {
      // Configure SparkSession if spark block is present
      if (config.hasPath("spark")) {
        val sparkConfig: SparkConfig = ConfigSource.fromConfig(config.getConfig("spark")).loadOrThrow[SparkConfig]
        logger.info("Configuring SparkSession from config")
        SparkSessionWrapper.configure(sparkConfig)
      }

      // Load pipeline configuration
      val pipelineConfig: PipelineConfig =
        ConfigSource.fromConfig(config.getConfig("pipeline")).loadOrThrow[PipelineConfig]

      logger.info(s"Starting pipeline: ${pipelineConfig.pipelineName}")
      logger.info(s"Components to run: ${pipelineConfig.pipelineComponents.size}")

      // Execute each component in sequence
      pipelineConfig.pipelineComponents.zipWithIndex.foreach {
        case (componentConfig: ComponentConfig, index: Int) =>
          val componentNumber: Int = index + 1
          val totalComponents: Int = pipelineConfig.pipelineComponents.size

          logger.info(s"[$componentNumber/$totalComponents] Instantiating: ${componentConfig.instanceName}")
          logger.debug(s"Instance type: ${componentConfig.instanceType}")

          val component: PipelineComponent = ComponentInstantiator.instantiate[PipelineComponent](componentConfig)

          logger.info(s"[$componentNumber/$totalComponents] Running: ${componentConfig.instanceName}")
          val startTime: Long = System.currentTimeMillis()

          component.run()

          val duration: Long = System.currentTimeMillis() - startTime
          logger.info(s"[$componentNumber/$totalComponents] Completed: ${componentConfig.instanceName} (${duration}ms)")
      }

      logger.info(s"Pipeline completed successfully: ${pipelineConfig.pipelineName}")

    } catch {
      case e: ComponentInstantiationException =>
        logger.error(s"Failed to instantiate component: ${e.getMessage}", e)
        throw e
      case e: pureconfig.error.ConfigReaderException[_] =>
        logger.error(s"Configuration error: ${e.getMessage}", e)
        throw e
      case e: Exception =>
        logger.error(s"Pipeline failed: ${e.getMessage}", e)
        throw e
    } finally {
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
}
