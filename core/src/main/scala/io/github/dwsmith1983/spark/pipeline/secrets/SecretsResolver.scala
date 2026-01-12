package io.github.dwsmith1983.spark.pipeline.secrets

import com.typesafe.config.{Config, ConfigFactory}
import io.github.dwsmith1983.spark.pipeline.secrets.audit.{SecretsAuditLogger, NoOpSecretsAuditLogger}

import scala.util.{Failure, Success, Try}

/**
 * Main entry point for resolving secrets in configuration.
 *
 * Resolves secret references in HOCON configuration strings before parsing.
 * References use the format `$${secret:provider://path#key}`.
 *
 * Example usage:
 * {{{
 * val resolver = SecretsResolver.builder()
 *   .withProvider(EnvSecretsProvider())
 *   .withProvider(AwsSecretsProvider(region = "us-east-1"))
 *   .build()
 *
 * val config = resolver.resolveAndParse(configString)
 * }}}
 *
 * @param providers Map of provider scheme to provider instance
 * @param cache TTL cache for resolved secrets
 * @param auditLogger Logger for secret access events
 */
class SecretsResolver(
  providers: Map[String, SecretsProvider],
  cache: SecretsCache,
  auditLogger: SecretsAuditLogger
) {

  /**
   * Resolve all secret references in a configuration string.
   *
   * @param configString The configuration string with secret references
   * @return Success with resolved config string, Failure if any secret cannot be resolved
   */
  def resolve(configString: String): Try[String] = {
    val references = SecretsReference.findAll(configString)

    if (references.isEmpty) {
      Success(configString)
    } else {
      resolveReferences(configString, references)
    }
  }

  /**
   * Resolve secrets and parse as Config.
   *
   * @param configString The configuration string with secret references
   * @return Success with parsed Config, Failure if resolution or parsing fails
   */
  def resolveAndParse(configString: String): Try[Config] =
    resolve(configString).flatMap { resolved =>
      Try(ConfigFactory.parseString(resolved))
    }

  /**
   * Resolve a single secret reference.
   *
   * @param ref The secret reference to resolve
   * @return Success with resolved value, Failure if resolution fails
   */
  def resolveReference(ref: SecretsReference): Try[SecretResolutionResult] = {
    providers.get(ref.provider) match {
      case None =>
        val error = new NoProviderException(ref.provider, providers.keys.toSeq)
        auditLogger.logAccess(auditLogger.createErrorEvent(ref, error))
        Failure(new SecretResolutionException(ref, error))

      case Some(provider) =>
        val cacheKey = cache.buildKey(ref.provider, ref.path, ref.key)

        cache.get(cacheKey) match {
          case Some(value) =>
            auditLogger.logAccess(auditLogger.createAccessEvent(ref, cached = true))
            Success(SecretResolutionResult(value, cached = true, ref.provider))

          case None =>
            provider.resolve(ref.path, ref.key) match {
              case Success(value) =>
                cache.put(cacheKey, value)
                auditLogger.logAccess(auditLogger.createAccessEvent(ref, cached = false))
                Success(SecretResolutionResult(value, cached = false, ref.provider))

              case Failure(error) =>
                auditLogger.logAccess(auditLogger.createErrorEvent(ref, error))
                Failure(new SecretResolutionException(ref, error))
            }
        }
    }
  }

  /**
   * Check if all configured providers are available.
   *
   * @return Map of provider scheme to availability status
   */
  def checkProviders(): Map[String, Boolean] =
    providers.map { case (scheme, provider) =>
      scheme -> provider.isAvailable
    }

  /**
   * Get list of available provider schemes.
   *
   * @return Sequence of available provider schemes
   */
  def availableSchemes: Seq[String] = providers.keys.toSeq.sorted

  /**
   * Close all providers and clear cache.
   */
  def close(): Unit = {
    providers.values.foreach(_.close())
    cache.invalidateAll()
  }

  private def resolveReferences(
    configString: String,
    references: List[String]
  ): Try[String] = {
    val resolutions = references.distinct.map { refString =>
      SecretsReference.parse(refString).flatMap { ref =>
        resolveReference(ref).map(result => refString -> result.value)
      }
    }

    val failures = resolutions.collect { case Failure(e) => e }

    if (failures.nonEmpty) {
      Failure(failures.head)
    } else {
      val replacements = resolutions.collect { case Success(pair) => pair }.toMap
      Success(
        replacements.foldLeft(configString) { case (config, (ref, value)) =>
          config.replace(ref, value)
        }
      )
    }
  }
}

/**
 * Builder for constructing SecretsResolver instances.
 */
class SecretsResolverBuilder {
  private var providers: Map[String, SecretsProvider] = Map.empty
  private var cache: SecretsCache = SecretsCache()
  private var auditLogger: SecretsAuditLogger = NoOpSecretsAuditLogger

  /**
   * Add a secrets provider.
   *
   * @param provider The provider to add
   * @return this builder
   */
  def withProvider(provider: SecretsProvider): SecretsResolverBuilder = {
    providers = providers + (provider.scheme -> provider)
    this
  }

  /**
   * Set the secrets cache.
   *
   * @param secretsCache The cache to use
   * @return this builder
   */
  def withCache(secretsCache: SecretsCache): SecretsResolverBuilder = {
    cache = secretsCache
    this
  }

  /**
   * Set the audit logger.
   *
   * @param logger The audit logger to use
   * @return this builder
   */
  def withAuditLogger(logger: SecretsAuditLogger): SecretsResolverBuilder = {
    auditLogger = logger
    this
  }

  /**
   * Build the SecretsResolver.
   *
   * @return Configured SecretsResolver
   */
  def build(): SecretsResolver =
    new SecretsResolver(providers, cache, auditLogger)
}

object SecretsResolver {

  /**
   * Create a new builder.
   *
   * @return SecretsResolverBuilder
   */
  def builder(): SecretsResolverBuilder = new SecretsResolverBuilder

  /**
   * Create a resolver with only environment variable provider.
   *
   * Useful for local development and testing.
   *
   * @return SecretsResolver with EnvSecretsProvider
   */
  def withEnvProvider(): SecretsResolver =
    builder()
      .withProvider(new providers.EnvSecretsProvider)
      .build()
}
