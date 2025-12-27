package io.github.dwsmith1983.spark.pipeline.audit

import com.typesafe.config.Config

import scala.annotation.nowarn

// Cross-version compatible Java collection converter
@nowarn("cat=deprecation")
private[audit] object JavaCompat {
  import scala.collection.JavaConverters._

  def configEntries(config: Config): Iterator[(String, String)] =
    config.entrySet().asScala.iterator.map(entry => entry.getKey -> entry.getValue.render())
}

/**
 * Filters configuration values before including in audit events.
 *
 * Used to redact sensitive information like passwords, API keys, and tokens
 * from audit logs.
 */
trait ConfigFilter {

  /**
   * Filter configuration values, redacting sensitive keys.
   *
   * @param config The raw configuration to filter
   * @return Map of key-value pairs with sensitive values redacted
   */
  def filter(config: Config): Map[String, String]
}

/** Factory methods and default implementations for ConfigFilter. */
object ConfigFilter {

  /** Default sensitive patterns that are always redacted */
  val DefaultSensitivePatterns: Set[String] = Set(
    "password",
    "secret",
    "key",
    "token",
    "credential",
    "auth",
    "private",
    "apikey",
    "api_key",
    "api-key"
  )

  /** Default filter that redacts common sensitive patterns */
  val default: ConfigFilter = new PatternConfigFilter(DefaultSensitivePatterns)

  /**
   * Creates a filter with custom sensitive patterns (replaces defaults).
   *
   * @param patterns Set of case-insensitive patterns to match against key names
   * @return ConfigFilter that redacts keys containing any of the patterns
   */
  def withPatterns(patterns: Set[String]): ConfigFilter =
    new PatternConfigFilter(patterns)

  /**
   * Creates a filter that extends the defaults with additional patterns.
   *
   * Use this when you want to keep the default patterns and add your own.
   *
   * {{{
   * val filter = ConfigFilter.withAdditionalPatterns(Set("mycompany_secret", "internal_key"))
   * // Redacts all defaults PLUS your custom patterns
   * }}}
   *
   * @param additionalPatterns Additional patterns to redact beyond the defaults
   * @return ConfigFilter that redacts defaults + additional patterns
   */
  def withAdditionalPatterns(additionalPatterns: Set[String]): ConfigFilter =
    new PatternConfigFilter(DefaultSensitivePatterns ++ additionalPatterns)

  /**
   * Filter that passes through all values without redaction.
   *
   * WARNING: Use only for testing. Do not use in production as it may
   * expose sensitive information in audit logs.
   */
  val passthrough: ConfigFilter = new ConfigFilter {

    override def filter(config: Config): Map[String, String] =
      JavaCompat.configEntries(config).toMap
  }
}

/**
 * Config filter that redacts values for keys matching sensitive patterns.
 *
 * Pattern matching is case-insensitive and checks if the key contains the pattern.
 *
 * @param sensitivePatterns Patterns to match against key names
 */
class PatternConfigFilter(sensitivePatterns: Set[String]) extends ConfigFilter {

  private val RedactedValue = "***REDACTED***"

  override def filter(config: Config): Map[String, String] =
    JavaCompat.configEntries(config).map {
      case (key, value) =>
        key -> (if (isSensitive(key)) RedactedValue else value)
    }.toMap

  private def isSensitive(key: String): Boolean = {
    val lowerKey = key.toLowerCase
    sensitivePatterns.exists(pattern => lowerKey.contains(pattern.toLowerCase))
  }
}
