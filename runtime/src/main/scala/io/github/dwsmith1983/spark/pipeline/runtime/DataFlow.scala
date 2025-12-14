package io.github.dwsmith1983.spark.pipeline.runtime

import io.github.dwsmith1983.spark.pipeline.config.PipelineComponent
import org.apache.logging.log4j.{LogManager, Logger}

/**
 * Base trait for Spark-based pipeline components.
 *
 * DataFlow combines:
 * - [[SparkSessionWrapper]] - provides access to `spark` session
 * - [[PipelineComponent]] - provides the `run()` contract
 * - Log4j2 logging via `logger`
 *
 * This is the primary trait that user pipeline components should extend.
 *
 * == Logging ==
 *
 * DataFlow provides a Log4j2 logger instance which supports:
 * - `logger.info(msg)` - Info level logging
 * - `logger.debug(msg)` - Debug level logging
 * - `logger.warn(msg)` - Warning level logging
 * - `logger.error(msg)` - Error level logging
 * - `logger.trace(msg)` - Trace level logging
 *
 * Logging is configured via Log4j2 (log4j2.properties).
 *
 * == Example ==
 *
 * {{{
 * import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
 * import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow
 * import pureconfig._
 * import pureconfig.generic.auto._
 *
 * object MyTransformation extends ConfigurableInstance {
 *   case class Config(
 *     inputTable: String,
 *     outputPath: String,
 *     writeFormat: String = "parquet"
 *   )
 *
 *   override def createFromConfig(conf: com.typesafe.config.Config): MyTransformation = {
 *     new MyTransformation(ConfigSource.fromConfig(conf).loadOrThrow[Config])
 *   }
 * }
 *
 * class MyTransformation(conf: MyTransformation.Config) extends DataFlow {
 *   import spark.implicits._
 *
 *   override def run(): Unit = {
 *     logger.info(s"Reading from table: ${conf.inputTable}")
 *
 *     val df = spark.table(conf.inputTable)
 *       .filter($"active" === true)
 *       .select("id", "name", "value")
 *
 *     logger.info(s"Writing to: ${conf.outputPath}")
 *     df.write
 *       .format(conf.writeFormat)
 *       .mode("overwrite")
 *       .save(conf.outputPath)
 *
 *     logger.info("Transformation complete")
 *   }
 * }
 * }}}
 */
trait DataFlow extends SparkSessionWrapper with PipelineComponent {

  /** Log4j2 logger instance for this component */
  protected val logger: Logger = LogManager.getLogger(getClass)

  // Inherits:
  // - spark: SparkSession (from SparkSessionWrapper)
  // - sparkContext: SparkContext (from SparkSessionWrapper)
  // - sqlContext: SQLContext (from SparkSessionWrapper)
  // - logger: Logger (Log4j2)
  // - name: String (from PipelineComponent, can override)
  // - run(): Unit (from PipelineComponent, must implement)
}
