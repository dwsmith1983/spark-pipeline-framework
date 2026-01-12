package io.github.dwsmith1983.spark.pipeline.secrets

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
 * Represents a parsed secret reference from configuration.
 *
 * Secret references use the format: `$${secret:provider://path#key}`
 *
 * Examples:
 * {{{
 * $${secret:aws://my-app/db-credentials#password}
 * $${secret:vault://database/creds/readonly#username}
 * $${secret:env://DB_PASSWORD}
 * }}}
 *
 * @param provider The secrets provider scheme (aws, vault, env)
 * @param path The path to the secret in the provider
 * @param key Optional key within the secret (for JSON secrets)
 * @param original The original reference string for error messages
 */
case class SecretsReference(
  provider: String,
  path: String,
  key: Option[String],
  original: String)

/** Companion object for parsing secret references. */
object SecretsReference {

  /**
   * Pattern matching secret references in configuration.
   *
   * Format: `$${secret:provider://path#key}` or `$${secret:provider://path}`
   *
   * Groups:
   *  - Group 1: provider (aws, vault, env)
   *  - Group 2: path
   *  - Group 3: key (optional)
   */
  val ReferencePattern: Regex =
    """\$\{secret:([a-z]+)://([^#}]+)(?:#([^}]+))?\}""".r

  /**
   * Parse a secret reference string.
   *
   * @param reference The reference string to parse
   * @return Success with parsed reference, or Failure if invalid format
   */
  def parse(reference: String): Try[SecretsReference] =
    reference match {
      case ReferencePattern(provider, path, keyOrNull) =>
        Success(
          SecretsReference(
            provider = provider,
            path = path.trim,
            key = Option(keyOrNull).map(_.trim),
            original = reference
          )
        )
      case _ =>
        val expectedFormat = "$" + "{secret:provider://path} or $" + "{secret:provider://path#key}"
        Failure(
          new IllegalArgumentException(
            s"Invalid secret reference format: $reference. Expected format: $expectedFormat"
          )
        )
    }

  /**
   * Find all secret references in a configuration string.
   *
   * @param config The configuration string to search
   * @return List of all secret references found (as raw strings)
   */
  def findAll(config: String): List[String] =
    ReferencePattern.findAllIn(config).toList

  /**
   * Parse all secret references in a configuration string.
   *
   * @param config The configuration string to search
   * @return List of parsed references (failures are filtered out)
   */
  def parseAll(config: String): List[SecretsReference] =
    findAll(config).flatMap(ref => parse(ref).toOption)

  /**
   * Check if a string contains any secret references.
   *
   * @param config The configuration string to check
   * @return true if secret references are present
   */
  def containsReferences(config: String): Boolean =
    ReferencePattern.findFirstIn(config).isDefined
}
