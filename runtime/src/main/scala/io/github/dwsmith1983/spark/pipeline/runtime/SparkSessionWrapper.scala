package io.github.dwsmith1983.spark.pipeline.runtime

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import io.github.dwsmith1983.spark.pipeline.config.SparkConfig

/**
 * Trait providing access to a SparkSession.
 *
 * Classes extending this trait get lazy access to a pre-configured SparkSession.
 * The session is configured by the SimplePipelineRunner before components are
 * instantiated, based on the `spark` block in the configuration file.
 *
 * Example:
 * {{{
 * class MyComponent extends SparkSessionWrapper {
 *   def process(): Unit = {
 *     import spark.implicits._
 *     val df = spark.read.parquet("/data/input")
 *     // ...
 *   }
 * }
 * }}}
 */
trait SparkSessionWrapper {

  /**
   * The SparkSession instance.
   * Lazily initialized from the configured session or creates a new one.
   */
  lazy val spark: SparkSession = SparkSessionWrapper.getOrCreate()

  /** Convenience access to SparkContext. */
  implicit lazy val sparkContext: SparkContext = spark.sparkContext

  /** Convenience access to SQLContext. */
  implicit lazy val sqlContext: SQLContext = spark.sqlContext
}

/**
 * Companion object managing the SparkSession lifecycle.
 *
 * The SimplePipelineRunner calls `configure()` before instantiating components
 * to set up the SparkSession based on configuration. Components then access
 * the session via `getOrCreate()`.
 */
object SparkSessionWrapper {
  @volatile private var configuredSession: Option[SparkSession] = None
  private val lock: Object                                      = new Object()

  /**
   * Configures the SparkSession from the given SparkConfig.
   *
   * This should be called by the SimplePipelineRunner before any components
   * are instantiated. Subsequent calls to `getOrCreate()` will return this
   * configured session.
   *
   * Automatically selects between local and Spark Connect modes based on configuration:
   * - If connectString is present: creates a remote Spark Connect session
   * - Otherwise: creates a traditional local/cluster session
   *
   * @param sparkConfig The Spark configuration from the config file
   * @return The configured SparkSession
   */
  def configure(sparkConfig: SparkConfig): SparkSession = lock.synchronized {
    val connector: SparkConnector = SparkConnector(sparkConfig)
    val session: SparkSession     = connector.getSession
    configuredSession = Some(session)
    session
  }

  /**
   * Gets the configured SparkSession, or creates a default one.
   *
   * If `configure()` has been called, returns that session.
   * Otherwise, creates a new session with `getOrCreate()`.
   *
   * @return The SparkSession
   */
  def getOrCreate(): SparkSession =
    configuredSession.getOrElse {
      lock.synchronized {
        configuredSession.getOrElse {
          val session: SparkSession = SparkSession.builder().getOrCreate()
          configuredSession = Some(session)
          session
        }
      }
    }

  /**
   * Clears the configured session.
   * Primarily for testing purposes.
   */
  def reset(): Unit = lock.synchronized {
    configuredSession = None
  }

  /** Stops the SparkSession and clears the reference. */
  def stop(): Unit = lock.synchronized {
    configuredSession.foreach(_.stop())
    configuredSession = None
  }
}
