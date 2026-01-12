package io.github.dwsmith1983.spark.pipeline.secrets.providers

import io.github.dwsmith1983.spark.pipeline.secrets.SecretsProvider

import scala.util.{Failure, Success, Try}

/**
 * Secrets provider that reads from environment variables.
 *
 * Simple provider for local development, testing, and CI/CD environments.
 * Does not support the #key syntax since env vars are simple strings.
 *
 * Usage in config:
 * {{{
 * database {
 *   password = "$${secret:env://DB_PASSWORD}"
 *   api-key = "$${secret:env://API_KEY}"
 * }
 * }}}
 *
 * @param envProvider Function to get environment variables (injectable for testing)
 */
class EnvSecretsProvider(
  envProvider: String => Option[String] = EnvSecretsProvider.systemEnv
) extends SecretsProvider {

  override val scheme: String = "env"

  override def resolve(path: String, key: Option[String]): Try[String] = {
    if (key.isDefined) {
      Failure(
        new IllegalArgumentException(
          s"Environment variable provider does not support #key syntax. " +
            s"Use env://$path instead of env://$path#${key.get}"
        )
      )
    } else {
      envProvider(path) match {
        case Some(value) => Success(value)
        case None =>
          Failure(
            new NoSuchElementException(
              s"Environment variable not found: $path"
            )
          )
      }
    }
  }

  override def isAvailable: Boolean = true

  override def close(): Unit = ()
}

object EnvSecretsProvider {

  /**
   * Default environment variable provider using System.getenv.
   */
  val systemEnv: String => Option[String] =
    name => Option(System.getenv(name))

  /**
   * Create a provider with custom environment.
   *
   * Useful for testing.
   *
   * @param env Map of environment variables
   * @return EnvSecretsProvider with custom environment
   */
  def withEnv(env: Map[String, String]): EnvSecretsProvider =
    new EnvSecretsProvider(name => env.get(name))

  /**
   * Create a provider with system environment.
   *
   * @return EnvSecretsProvider using System.getenv
   */
  def apply(): EnvSecretsProvider = new EnvSecretsProvider()
}
