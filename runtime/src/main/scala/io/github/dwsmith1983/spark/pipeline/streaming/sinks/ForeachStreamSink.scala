package io.github.dwsmith1983.spark.pipeline.streaming.sinks

import io.github.dwsmith1983.spark.pipeline.config.ConfigurableInstance
import io.github.dwsmith1983.spark.pipeline.streaming.StreamingSink
import io.github.dwsmith1983.spark.pipeline.streaming.config.{ForeachMode, ForeachSinkConfig}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import pureconfig._
import pureconfig.generic.auto._

/**
 * Trait for custom batch-level processing in streaming pipelines.
 *
 * Implement this trait to process entire micro-batches. This is useful
 * for database upserts, batch API calls, or complex transformations.
 *
 * == Example ==
 *
 * {{{
 * class MyBatchProcessor extends StreamBatchProcessor {
 *   override def processBatch(df: DataFrame, batchId: Long): Unit = {
 *     // Write to external database
 *     df.write
 *       .format("jdbc")
 *       .option("url", "jdbc:postgresql://localhost/db")
 *       .option("dbtable", "events")
 *       .mode("append")
 *       .save()
 *   }
 * }
 * }}}
 */
trait StreamBatchProcessor extends Serializable {

  /**
   * Process a micro-batch of streaming data.
   *
   * @param df The DataFrame for this micro-batch
   * @param batchId The unique ID for this batch
   */
  def processBatch(df: DataFrame, batchId: Long): Unit

  /**
   * Called when the streaming query terminates.
   *
   * Override to perform cleanup operations.
   */
  def onQueryTermination(): Unit = {}
}

/**
 * Trait for custom row-level processing in streaming pipelines.
 *
 * Implement this trait to process individual rows. This is useful
 * for custom serialization or when batch processing is not suitable.
 *
 * == Example ==
 *
 * {{{
 * class MyRowProcessor extends StreamRowProcessor {
 *   private var connection: Connection = _
 *
 *   override def open(partitionId: Long, epochId: Long): Boolean = {
 *     connection = DriverManager.getConnection(...)
 *     true
 *   }
 *
 *   override def process(row: Row): Unit = {
 *     val stmt = connection.prepareStatement("INSERT INTO events VALUES (?)")
 *     stmt.setString(1, row.getString(0))
 *     stmt.execute()
 *   }
 *
 *   override def close(error: Throwable): Unit = {
 *     if (connection != null) connection.close()
 *   }
 * }
 * }}}
 */
trait StreamRowProcessor extends Serializable {

  /**
   * Called when a partition is opened for processing.
   *
   * @param partitionId The partition ID
   * @param epochId The epoch (batch) ID
   * @return true to process this partition, false to skip
   */
  def open(partitionId: Long, epochId: Long): Boolean = {
    val _ = (partitionId, epochId) // suppress unused warning - meant to be overridden
    true
  }

  /**
   * Process a single row.
   *
   * @param row The row to process
   */
  def process(row: Row): Unit

  /**
   * Called when the partition is closed.
   *
   * @param error Any error that occurred, or null if successful
   */
  def close(error: Throwable): Unit = {
    val _ = error // suppress unused warning - meant to be overridden
  }
}

/**
 * Streaming sink that invokes custom processing logic.
 *
 * This sink allows arbitrary processing by delegating to user-provided
 * processor classes. It supports both batch-level (`foreachBatch`) and
 * row-level (`foreach`) processing modes.
 *
 * == Batch Mode ==
 *
 * In batch mode, the processor receives entire micro-batches as DataFrames.
 * This is efficient for bulk operations like database writes or API calls.
 *
 * == Row Mode ==
 *
 * In row mode, the processor receives individual rows. This is useful
 * for custom serialization or fine-grained control over processing.
 *
 * == Processor Instantiation ==
 *
 * The processor class is instantiated via reflection. It must have either:
 * - A no-argument constructor, or
 * - A constructor that accepts `Map[String, String]` for configuration
 *
 * == Example ==
 *
 * {{{
 * val config = ForeachSinkConfig(
 *   processorClass = "com.example.MyBatchProcessor",
 *   checkpointPath = "/checkpoints/foreach",
 *   mode = "batch",
 *   processorConfig = Map("batchSize" -> "1000")
 * )
 * val sink = new ForeachStreamSink(config)
 * }}}
 *
 * @param config The foreach sink configuration
 */
class ForeachStreamSink(config: ForeachSinkConfig) extends StreamingSink {

  override def writeStream(df: DataFrame): DataStreamWriter[Row] = {
    logger.info(
      s"Creating foreach sink: processor=${config.processorClass}, " +
        s"mode=${config.mode}"
    )

    config.foreachMode match {
      case ForeachMode.Batch =>
        val processor = instantiateBatchProcessor()
        df.writeStream.foreachBatch((batchDf: DataFrame, batchId: Long) => processor.processBatch(batchDf, batchId))

      case ForeachMode.Row =>
        val processor = instantiateRowProcessor()
        df.writeStream.foreach(new ForeachWriter[Row] {
          override def open(partitionId: Long, epochId: Long): Boolean =
            processor.open(partitionId, epochId)

          override def process(row: Row): Unit =
            processor.process(row)

          override def close(error: Throwable): Unit =
            processor.close(error)
        })
    }
  }

  private def instantiateBatchProcessor(): StreamBatchProcessor =
    instantiateProcessor[StreamBatchProcessor](config.processorClass)

  private def instantiateRowProcessor(): StreamRowProcessor =
    instantiateProcessor[StreamRowProcessor](config.processorClass)

  private def instantiateProcessor[T](className: String): T = {
    val clazz = Class.forName(className)

    val instance =
      try {
        val configConstructor = clazz.getConstructor(classOf[Map[_, _]])
        configConstructor.newInstance(config.processorConfig)
      } catch {
        case _: NoSuchMethodException =>
          clazz.getDeclaredConstructor().newInstance()
      }

    instance.asInstanceOf[T]
  }

  override def outputMode: OutputMode = OutputMode.Append()

  override def checkpointLocation: String = config.checkpointPath

  override def queryName: Option[String] = config.queryName
}

/** Factory for creating ForeachStreamSink instances from HOCON configuration. */
object ForeachStreamSink extends ConfigurableInstance {

  override def createFromConfig(conf: com.typesafe.config.Config): ForeachStreamSink =
    new ForeachStreamSink(ConfigSource.fromConfig(conf).loadOrThrow[ForeachSinkConfig])
}
