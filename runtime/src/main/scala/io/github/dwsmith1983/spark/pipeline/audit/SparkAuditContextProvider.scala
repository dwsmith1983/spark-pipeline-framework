package io.github.dwsmith1983.spark.pipeline.audit

import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper

import java.net.InetAddress

/**
 * Spark-aware context provider for audit events.
 *
 * Extracts execution context from the active SparkSession, including
 * application ID, master URL, Spark version, and filtered configuration.
 *
 * == Usage ==
 *
 * {{{
 * import io.github.dwsmith1983.spark.pipeline.audit._
 *
 * val hooks = AuditHooks(
 *   sink = AuditSink.file("/var/log/audit.jsonl"),
 *   contextProvider = SparkAuditContextProvider()
 * )
 * }}}
 *
 * == With Custom Config Filtering ==
 *
 * {{{
 * val hooks = AuditHooks(
 *   sink = AuditSink.file("/var/log/audit.jsonl"),
 *   contextProvider = SparkAuditContextProvider.withConfigFilter(
 *     ConfigFilter.withAdditionalPatterns(Set("internal_secret"))
 *   )
 * )
 * }}}
 *
 * @param sparkConfigFilter Filter for Spark configuration properties
 */
class SparkAuditContextProvider(
  val sparkConfigFilter: ConfigFilter = ConfigFilter.default)
  extends AuditContextProvider {

  override def getSystemContext(envFilter: EnvFilter): SystemContext = {
    val hostname =
      try {
        InetAddress.getLocalHost.getHostName
      } catch {
        case _: Exception => "unknown"
      }

    val (sparkVersion, appId) =
      try {
        val spark = SparkSessionWrapper.getOrCreate()
        val sc    = spark.sparkContext
        (Some(sc.version), Some(sc.applicationId))
      } catch {
        case _: Exception => (None, None)
      }

    SystemContext(
      hostname = hostname,
      jvmVersion = System.getProperty("java.version", "unknown"),
      scalaVersion = util.Properties.versionString,
      sparkVersion = sparkVersion,
      applicationId = appId,
      executorId = Some("driver"), // Always driver in the context where hooks run
      environment = envFilter.filter(sys.env)
    )
  }

  override def getSparkContext(): Option[SparkExecutionContext] =
    try {
      val spark = SparkSessionWrapper.getOrCreate()
      val sc    = spark.sparkContext
      val conf  = sc.getConf

      // Filter Spark properties using the provided filter's pattern logic
      // We need to manually filter since SparkConf isn't a typesafe Config
      val allProps = conf.getAll.toMap
      val filteredProps = allProps.filter {
        case (key, _) =>
          !isSensitiveSparkKey(key)
      }

      Some(
        SparkExecutionContext(
          applicationId = sc.applicationId,
          applicationName = Some(sc.appName),
          master = Some(sc.master),
          sparkVersion = sc.version,
          sparkProperties = filteredProps
        )
      )
    } catch {
      case _: Exception => None
    }

  private def isSensitiveSparkKey(key: String): Boolean = {
    val lowerKey = key.toLowerCase
    ConfigFilter.DefaultSensitivePatterns.exists(pattern => lowerKey.contains(pattern.toLowerCase))
  }
}

/** Factory methods for SparkAuditContextProvider. */
object SparkAuditContextProvider {

  /**
   * Creates a SparkAuditContextProvider with default settings.
   *
   * @return SparkAuditContextProvider that filters sensitive Spark properties
   */
  def apply(): SparkAuditContextProvider = new SparkAuditContextProvider()

  /**
   * Creates a SparkAuditContextProvider with a custom config filter.
   *
   * @param filter Custom filter for Spark configuration properties
   * @return SparkAuditContextProvider with custom filtering
   */
  def withConfigFilter(filter: ConfigFilter): SparkAuditContextProvider =
    new SparkAuditContextProvider(filter)
}
