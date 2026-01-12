package io.github.dwsmith1983.spark.pipeline.secrets

import scala.util.Try

/**
 * Base trait for secrets providers.
 *
 * Each provider implementation handles a specific secrets backend
 * (AWS Secrets Manager, HashiCorp Vault, environment variables, etc.).
 *
 * Implementations should be thread-safe as they may be called concurrently
 * during configuration resolution.
 *
 * Example implementation:
 * {{{
 * class MyProvider extends SecretsProvider {
 *   override def scheme: String = "my"
 *   override def resolve(path: String, key: Option[String]): Try[String] = {
 *     // Fetch secret from backend
 *   }
 * }
 * }}}
 */
trait SecretsProvider {

  /**
   * The URI scheme this provider handles.
   *
   * Used to match secret references like `$${secret:scheme://path}`.
   * Examples: "aws", "vault", "env", "azure"
   */
  def scheme: String

  /**
   * Resolve a secret value from the provider.
   *
   * @param path The path to the secret in the provider
   * @param key Optional key within the secret (for JSON/map secrets)
   * @return Success with the secret value, or Failure with error details
   */
  def resolve(path: String, key: Option[String]): Try[String]

  /**
   * Check if this provider is available and configured.
   *
   * Used for health checks and graceful degradation.
   * Should not throw exceptions.
   *
   * @return true if the provider can be used
   */
  def isAvailable: Boolean

  /**
   * Close any resources held by this provider.
   *
   * Called when the secrets resolver is shut down.
   * Should be idempotent.
   */
  def close(): Unit
}

/**
 * Result of a secret resolution attempt.
 *
 * @param value The resolved secret value (if successful)
 * @param cached Whether the value came from cache
 * @param provider The provider that resolved the secret
 */
case class SecretResolutionResult(
  value: String,
  cached: Boolean,
  provider: String)

/**
 * Exception thrown when a secret cannot be resolved.
 *
 * @param reference The secret reference that failed
 * @param cause The underlying cause
 */
class SecretResolutionException(
  val reference: SecretsReference,
  cause: Throwable
) extends RuntimeException(
      s"Failed to resolve secret: ${reference.original}",
      cause
    )

/**
 * Exception thrown when no provider is available for a scheme.
 *
 * @param scheme The requested scheme
 * @param availableSchemes List of available schemes
 */
class NoProviderException(
  val scheme: String,
  val availableSchemes: Seq[String]
) extends RuntimeException(
      s"No secrets provider for scheme '$scheme'. " +
        s"Available: ${availableSchemes.mkString(", ")}"
    )
