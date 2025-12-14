package io.github.dwsmith1983.pipelines

import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.runtime.DataFlow
import com.typesafe.config.Config
import pureconfig._
import pureconfig.generic.auto._

/**
 * Example pipeline component that performs a word count.
 *
 * This demonstrates the pattern for creating pipeline components:
 * 1. Define a companion object extending ConfigurableInstance
 * 2. Define a case class for configuration
 * 3. Implement createFromConfig to parse config and create instance
 * 4. Extend DataFlow and implement run()
 */
object WordCount extends ConfigurableInstance {

  /**
   * Configuration for the WordCount component.
   *
   * @param inputPath Path to input text file(s)
   * @param outputPath Path to write word count results
   * @param minCount Minimum count threshold (optional, default 1)
   * @param writeFormat Output format (optional, default parquet)
   */
  case class WordCountConfig(
    inputPath: String,
    outputPath: String,
    minCount: Int = 1,
    writeFormat: String = "parquet")

  override def createFromConfig(conf: Config): WordCount =
    new WordCount(ConfigSource.fromConfig(conf).loadOrThrow[WordCountConfig])
}

/** Word count implementation extending DataFlow. */
class WordCount(conf: WordCount.WordCountConfig) extends DataFlow {
  import spark.implicits._

  override val name: String = "WordCount"

  override def run(): Unit = {
    logger.info(s"Reading text from: ${conf.inputPath}")

    // Read text files
    val textDF = spark.read.text(conf.inputPath)

    // Perform word count
    val wordCounts = textDF
      .selectExpr("explode(split(value, '\\\\s+')) as word")
      .filter($"word" =!= "")
      .groupBy($"word")
      .count()
      .filter($"count" >= conf.minCount)
      .orderBy($"count".desc)

    logger.info(s"Writing results to: ${conf.outputPath}")

    wordCounts.write
      .format(conf.writeFormat)
      .mode("overwrite")
      .save(conf.outputPath)

    logger.info(s"Word count complete. Found ${wordCounts.count()} unique words.")
  }
}
