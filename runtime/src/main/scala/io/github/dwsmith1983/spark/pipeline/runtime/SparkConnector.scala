package io.github.dwsmith1983.spark.pipeline.runtime

import org.apache.spark.sql.SparkSession
import io.github.dwsmith1983.spark.pipeline.config.SparkConfig

/**
 * Trait for connecting to Spark sessions.
 *
 * Abstracts the difference between local SparkSession creation
 * and remote Spark Connect sessions.
 */
trait SparkConnector {

  /**
   * Creates or retrieves a SparkSession.
   *
   * @return The SparkSession instance
   */
  def getSession: SparkSession
}

/**
 * Connector for local/cluster SparkSessions.
 *
 * Uses the traditional SparkSession.Builder with master configuration.
 * This is the backward-compatible mode that works with existing configurations.
 *
 * @param sparkConfig The Spark configuration
 */
class LocalSparkConnector(sparkConfig: SparkConfig) extends SparkConnector {

  override def getSession: SparkSession = {
    val builder: SparkSession.Builder = SparkSession.builder()

    // Apply master if specified
    sparkConfig.master.foreach(m => builder.master(m))

    // Apply app name if specified
    sparkConfig.appName.foreach(name => builder.appName(name))

    // Apply all additional Spark config
    sparkConfig.config.foreach {
      case (key, value) =>
        builder.config(key, value)
    }

    builder.getOrCreate()
  }
}

/**
 * Connector for Spark Connect remote sessions.
 *
 * Uses Spark Connect protocol (Spark 3.4+) to connect to remote Spark clusters.
 * Supports both standard Spark Connect servers and Databricks Connect.
 *
 * Connection string format: sc://host:port
 * Example: sc://spark-server:15002
 * Example: sc://your-workspace.cloud.databricks.com
 *
 * @note Requires Spark 3.4 or later. Will throw an exception if used with earlier versions.
 * @param sparkConfig The Spark configuration with connectString
 */
class SparkConnectConnector(sparkConfig: SparkConfig) extends SparkConnector {

  override def getSession: SparkSession = {
    require(sparkConfig.connectString.isDefined, "connectString must be defined for SparkConnectConnector")

    val builder: SparkSession.Builder = SparkSession.builder()
    val connectionString: String      = sparkConfig.connectString.get

    // Use reflection to call builder.remote(connectionString) to support Spark 3.4+
    // while maintaining compatibility with earlier versions at compile time
    try {
      val remoteMethod: java.lang.reflect.Method = builder.getClass.getMethod("remote", classOf[String])
      remoteMethod.invoke(builder, connectionString)
    } catch {
      case _: NoSuchMethodException =>
        throw new UnsupportedOperationException(
          "Spark Connect requires Spark 3.4 or later. " +
            s"Current Spark version does not support the remote() method. " +
            s"Connection string: $connectionString"
        )
      case e: Exception =>
        throw new RuntimeException(s"Failed to configure Spark Connect with connection string: $connectionString", e)
    }

    // Apply app name if specified
    sparkConfig.appName.foreach(name => builder.appName(name))

    // Apply Databricks token if specified
    sparkConfig.databricksToken.foreach(token => builder.config("spark.databricks.token", token))

    // Apply all additional Spark config
    sparkConfig.config.foreach {
      case (key, value) =>
        builder.config(key, value)
    }

    builder.getOrCreate()
  }
}

/** Factory for creating SparkConnectors based on configuration. */
object SparkConnector {

  /**
   * Creates the appropriate SparkConnector based on SparkConfig.
   *
   * Selection logic:
   * - If connectString is present → SparkConnectConnector (remote mode)
   * - Otherwise → LocalSparkConnector (local/cluster mode)
   *
   * @param sparkConfig The Spark configuration
   * @return The appropriate SparkConnector implementation
   */
  def apply(sparkConfig: SparkConfig): SparkConnector =
    sparkConfig.connectString match {
      case Some(_) => new SparkConnectConnector(sparkConfig)
      case None    => new LocalSparkConnector(sparkConfig)
    }
}
