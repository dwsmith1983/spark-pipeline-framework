package io.github.dwsmith1983.spark.pipeline.audit

/**
 * Filters environment variables before including in audit events.
 *
 * Supports both allowlist (include only specified keys) and denylist
 * (exclude keys matching patterns) modes.
 */
trait EnvFilter {

  /**
   * Filter environment variables.
   *
   * @param env The environment variables to filter
   * @return Filtered map of environment variables
   */
  def filter(env: Map[String, String]): Map[String, String]
}

/** Factory methods and default implementations for EnvFilter. */
object EnvFilter {

  /** Default allowlist of common safe environment variables */
  val DefaultAllowKeys: Set[String] = Set(
    "SPARK_HOME",
    "HADOOP_HOME",
    "HADOOP_CONF_DIR",
    "JAVA_HOME",
    "SCALA_HOME",
    "PATH",
    "USER",
    "HOME",
    "HOSTNAME",
    "PWD",
    "SHELL"
  )

  /** Default deny patterns for sensitive environment variables */
  val DefaultDenyPatterns: Set[String] = Set(
    "PASSWORD",
    "SECRET",
    "KEY",
    "TOKEN",
    "CREDENTIAL",
    "AUTH",
    "PRIVATE",
    "AWS_SECRET",
    "API_KEY"
  )

  /** Default filter using allowlist of common safe variables */
  val default: EnvFilter = allowlist(DefaultAllowKeys)

  /**
   * Creates a filter that includes only specified keys (replaces defaults).
   *
   * @param keys Set of environment variable names to include
   * @return EnvFilter that includes only the specified keys
   */
  def allowlist(keys: Set[String]): EnvFilter = new AllowlistEnvFilter(keys)

  /**
   * Creates a filter that extends the default allowlist with additional keys.
   *
   * Use this when you want to keep the defaults and add your own.
   *
   * {{{
   * val filter = EnvFilter.withAdditionalAllowedKeys(Set("MY_APP_ENV", "DEPLOYMENT_REGION"))
   * // Includes all defaults PLUS your custom keys
   * }}}
   *
   * @param additionalKeys Additional keys to include beyond the defaults
   * @return EnvFilter that includes defaults + additional keys
   */
  def withAdditionalAllowedKeys(additionalKeys: Set[String]): EnvFilter =
    new AllowlistEnvFilter(DefaultAllowKeys ++ additionalKeys)

  /**
   * Creates a filter that excludes keys matching patterns (replaces defaults).
   *
   * @param patterns Set of case-insensitive patterns to exclude
   * @return EnvFilter that excludes keys containing any pattern
   */
  def denylist(patterns: Set[String]): EnvFilter = new DenylistEnvFilter(patterns)

  /**
   * Creates a filter using denylist with defaults plus additional patterns.
   *
   * @param additionalPatterns Additional patterns to deny beyond the defaults
   * @return EnvFilter using denylist with defaults + additional patterns
   */
  def withAdditionalDenyPatterns(additionalPatterns: Set[String]): EnvFilter =
    new DenylistEnvFilter(DefaultDenyPatterns ++ additionalPatterns)

  /** Filter that includes nothing */
  val empty: EnvFilter = new EnvFilter {
    override def filter(env: Map[String, String]): Map[String, String] = Map.empty
  }

  /**
   * Filter that passes through all environment variables.
   *
   * WARNING: Use only for testing. Do not use in production as it may
   * expose sensitive information in audit logs.
   */
  val passthrough: EnvFilter = new EnvFilter {
    override def filter(env: Map[String, String]): Map[String, String] = env
  }
}

/**
 * Environment filter that includes only keys in the allowlist.
 *
 * @param allowedKeys Set of environment variable names to include
 */
class AllowlistEnvFilter(allowedKeys: Set[String]) extends EnvFilter {

  override def filter(env: Map[String, String]): Map[String, String] =
    env.filter { case (key, _) => allowedKeys.contains(key) }
}

/**
 * Environment filter that excludes keys matching deny patterns.
 *
 * Pattern matching is case-insensitive.
 *
 * @param denyPatterns Set of patterns to exclude
 */
class DenylistEnvFilter(denyPatterns: Set[String]) extends EnvFilter {

  override def filter(env: Map[String, String]): Map[String, String] =
    env.filter {
      case (key, _) =>
        !denyPatterns.exists(pattern => key.toUpperCase.contains(pattern.toUpperCase))
    }
}
