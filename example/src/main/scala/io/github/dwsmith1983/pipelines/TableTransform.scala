package io.github.dwsmith1983.pipelines

import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow
import com.typesafe.config.Config
import pureconfig._
import pureconfig.generic.auto._

/**
 * Example pipeline component that transforms a Hive/Spark table.
 *
 * This example shows how to work with tables and apply transformations
 */
object TableTransform extends ConfigurableInstance {

  /**
   * Configuration for table transformation.
   *
   * @param inputTable Input table name (database.table or just table)
   * @param outputPath Output path for results
   * @param selectColumns Columns to select (optional, default all)
   * @param filterCondition SQL filter condition (optional)
   * @param writeFormat Output format
   * @param writeMode Write mode (overwrite, append, etc.)
   */
  case class TransformConfig(
    inputTable: String,
    outputPath: String,
    selectColumns: List[String] = List.empty,
    filterCondition: Option[String] = None,
    writeFormat: String = "parquet",
    writeMode: String = "overwrite")

  override def createFromConfig(conf: Config): TableTransform =
    new TableTransform(ConfigSource.fromConfig(conf).loadOrThrow[TransformConfig])
}

/** Table transformation implementation. */
class TableTransform(conf: TableTransform.TransformConfig) extends DataFlow {

  override val name: String = s"TableTransform(${conf.inputTable})"

  override def run(): Unit = {
    logger.info(s"Reading from table: ${conf.inputTable}")

    // Read the input table
    var df = spark.table(conf.inputTable)

    // Apply column selection if specified
    if (conf.selectColumns.nonEmpty) {
      logger.info(s"Selecting columns: ${conf.selectColumns.mkString(", ")}")
      df = df.select(conf.selectColumns.map(df.col): _*)
    }

    // Apply filter if specified
    conf.filterCondition.foreach { condition =>
      logger.info(s"Applying filter: $condition")
      df = df.filter(condition)
    }

    logger.info(s"Writing to: ${conf.outputPath} (format=${conf.writeFormat}, mode=${conf.writeMode})")

    df.write
      .format(conf.writeFormat)
      .mode(conf.writeMode)
      .save(conf.outputPath)

    logger.info(s"Transform complete. Wrote ${df.count()} rows.")
  }
}
