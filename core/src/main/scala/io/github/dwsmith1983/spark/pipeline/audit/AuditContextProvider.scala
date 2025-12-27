package io.github.dwsmith1983.spark.pipeline.audit

import java.net.InetAddress

/**
 * Provides execution context for audit events.
 *
 * Implementations can extract context from various sources (JVM, Spark, etc.).
 * The default implementation provides JVM-only context without Spark.
 *
 * == Custom Implementation ==
 *
 * To provide Spark context, use the SparkAuditContextProvider from the runtime module,
 * or implement your own:
 *
 * {{{
 * class MyContextProvider extends AuditContextProvider {
 *   override def getSystemContext(envFilter: EnvFilter): SystemContext = {
 *     // Custom implementation
 *   }
 *
 *   override def getSparkContext(): Option[SparkExecutionContext] = {
 *     // Return Spark context if available
 *   }
 * }
 * }}}
 */
trait AuditContextProvider {

  /**
   * Get system context (always available, no Spark required).
   *
   * @param envFilter Filter for environment variables
   * @return SystemContext with JVM and host information
   */
  def getSystemContext(envFilter: EnvFilter): SystemContext

  /**
   * Get Spark execution context if Spark is available.
   *
   * @return Some(SparkExecutionContext) if Spark is running, None otherwise
   */
  def getSparkContext(): Option[SparkExecutionContext]
}

/**
 * Factory methods for AuditContextProvider.
 */
object AuditContextProvider {

  /** Default provider - no Spark context */
  val default: AuditContextProvider = new DefaultAuditContextProvider
}

/**
 * Default context provider that works without Spark.
 *
 * Provides JVM and system information only.
 */
class DefaultAuditContextProvider extends AuditContextProvider {

  override def getSystemContext(envFilter: EnvFilter): SystemContext = {
    val hostname = try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case _: Exception => "unknown"
    }

    SystemContext(
      hostname = hostname,
      jvmVersion = System.getProperty("java.version", "unknown"),
      scalaVersion = util.Properties.versionString,
      sparkVersion = None,
      applicationId = None,
      executorId = None,
      environment = envFilter.filter(sys.env)
    )
  }

  override def getSparkContext(): Option[SparkExecutionContext] = None
}
