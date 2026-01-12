package io.github.dwsmith1983.spark.pipeline.secrets.providers

import io.github.dwsmith1983.spark.pipeline.secrets.SecretsProvider

import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success, Try}

/**
 * Secrets provider for HashiCorp Vault.
 *
 * Supports KV secrets engine v2 (the default for new Vault installations).
 * Authenticates using a Vault token.
 *
 * Usage in config:
 * {{{
 * database {
 *   // Fetch specific key from secret
 *   password = "$${secret:vault://secret/data/database/prod#password}"
 *   username = "$${secret:vault://secret/data/database/prod#username}"
 * }
 * }}}
 *
 * Environment variables:
 * - VAULT_ADDR: Vault server address (e.g., https://vault.example.com:8200)
 * - VAULT_TOKEN: Authentication token
 *
 * @param client The Vault HTTP client
 */
class VaultSecretsProvider(client: VaultClient) extends SecretsProvider {

  override val scheme: String = "vault"

  override def resolve(path: String, key: Option[String]): Try[String] =
    client.readSecret(path).flatMap { secretData =>
      key match {
        case None =>
          // Return the entire data section as JSON
          Success(secretData)
        case Some(k) =>
          extractJsonKey(secretData, k, path)
      }
    }

  override def isAvailable: Boolean = client.isAvailable

  override def close(): Unit = client.close()

  private def extractJsonKey(json: String, key: String, path: String): Try[String] =
    Try {
      // Simple JSON key extraction
      val keyPattern = s""""${java.util.regex.Pattern.quote(key)}"\\s*:\\s*"?([^",}]+)"?""".r

      keyPattern.findFirstMatchIn(json) match {
        case Some(m) => m.group(1)
        case None =>
          throw new NoSuchElementException(
            s"Key '$key' not found in secret '$path'. " +
              "Ensure the secret contains the specified key."
          )
      }
    }
}

/**
 * Interface for Vault operations.
 */
trait VaultClient {

  /**
   * Read a secret from Vault.
   *
   * @param path The secret path (e.g., "secret/data/myapp/config")
   * @return Success with the data section of the secret, Failure on error
   */
  def readSecret(path: String): Try[String]

  /**
   * Check if the client is properly configured and can authenticate.
   *
   * @return true if Vault is reachable and token is valid
   */
  def isAvailable: Boolean

  /**
   * Close the client.
   */
  def close(): Unit
}

/**
 * HTTP-based Vault client.
 *
 * Uses standard Java HTTP to avoid external dependencies.
 *
 * @param address Vault server address
 * @param token Authentication token
 * @param namespace Optional Vault namespace (for Vault Enterprise)
 */
class HttpVaultClient(
  address: String,
  token: String,
  namespace: Option[String] = None
) extends VaultClient {

  private val baseUrl = address.stripSuffix("/")

  override def readSecret(path: String): Try[String] =
    Try {
      val normalizedPath = if (path.startsWith("/")) path.substring(1) else path
      val url = new URL(s"$baseUrl/v1/$normalizedPath")
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]

      try {
        connection.setRequestMethod("GET")
        connection.setRequestProperty("X-Vault-Token", token)
        namespace.foreach(ns => connection.setRequestProperty("X-Vault-Namespace", ns))
        connection.setConnectTimeout(5000)
        connection.setReadTimeout(10000)

        val responseCode = connection.getResponseCode

        if (responseCode == 200) {
          val reader = new BufferedReader(
            new InputStreamReader(connection.getInputStream, StandardCharsets.UTF_8)
          )
          try {
            val response = new StringBuilder
            var line: String = reader.readLine()
            while (line != null) {
              response.append(line)
              line = reader.readLine()
            }
            extractDataSection(response.toString, path)
          } finally {
            reader.close()
          }
        } else if (responseCode == 404) {
          throw new NoSuchElementException(s"Secret not found: $path")
        } else if (responseCode == 403) {
          throw new SecurityException(
            s"Access denied to secret: $path. Check Vault token and policies."
          )
        } else {
          throw new RuntimeException(
            s"Vault returned HTTP $responseCode for secret: $path"
          )
        }
      } finally {
        connection.disconnect()
      }
    }

  override def isAvailable: Boolean =
    try {
      val url = new URL(s"$baseUrl/v1/sys/health")
      val connection = url.openConnection().asInstanceOf[HttpURLConnection]
      try {
        connection.setRequestMethod("GET")
        connection.setConnectTimeout(3000)
        connection.setReadTimeout(3000)
        val code = connection.getResponseCode
        // Vault health returns various codes for different states
        // 200 = initialized, unsealed, active
        // 429 = unsealed, standby
        // 472 = DR secondary
        // 473 = performance standby
        code == 200 || code == 429 || code == 472 || code == 473
      } finally {
        connection.disconnect()
      }
    } catch {
      case _: Exception => false
    }

  override def close(): Unit = ()

  private def extractDataSection(json: String, path: String): String = {
    // Extract the "data" section from KV v2 response
    // Response format: {"data": {"data": {...}, "metadata": {...}}}
    // We want the inner "data" section

    // For KV v2, look for "data":{"data": pattern
    val kvV2Pattern = """"data"\s*:\s*\{\s*"data"\s*:\s*(\{[^}]+\})""".r
    val kvV1Pattern = """"data"\s*:\s*(\{[^}]+\})""".r

    kvV2Pattern.findFirstMatchIn(json).map(_.group(1))
      .orElse(kvV1Pattern.findFirstMatchIn(json).map(_.group(1)))
      .getOrElse {
        throw new RuntimeException(
          s"Unable to parse Vault response for secret: $path"
        )
      }
  }
}

/**
 * Mock Vault client for testing.
 *
 * @param secrets Map of path to secret data (JSON string)
 */
class MockVaultClient(secrets: Map[String, String]) extends VaultClient {

  override def readSecret(path: String): Try[String] =
    secrets.get(path) match {
      case Some(value) => Success(value)
      case None =>
        Failure(new NoSuchElementException(s"Secret not found: $path"))
    }

  override def isAvailable: Boolean = true

  override def close(): Unit = ()
}

object VaultSecretsProvider {

  /**
   * Create a Vault provider using environment variables.
   *
   * Reads VAULT_ADDR and VAULT_TOKEN from environment.
   *
   * @return Try containing the provider or failure if env vars missing
   */
  def fromEnv(): Try[VaultSecretsProvider] = {
    val addressOpt = Option(System.getenv("VAULT_ADDR"))
    val tokenOpt = Option(System.getenv("VAULT_TOKEN"))
    val namespace = Option(System.getenv("VAULT_NAMESPACE"))

    (addressOpt, tokenOpt) match {
      case (Some(address), Some(token)) =>
        Success(new VaultSecretsProvider(new HttpVaultClient(address, token, namespace)))
      case (None, _) =>
        Failure(new IllegalStateException("VAULT_ADDR environment variable not set"))
      case (_, None) =>
        Failure(new IllegalStateException("VAULT_TOKEN environment variable not set"))
    }
  }

  /**
   * Create a Vault provider with explicit configuration.
   *
   * @param address Vault server address
   * @param token Authentication token
   * @param namespace Optional Vault namespace
   * @return VaultSecretsProvider
   */
  def apply(
    address: String,
    token: String,
    namespace: Option[String] = None
  ): VaultSecretsProvider =
    new VaultSecretsProvider(new HttpVaultClient(address, token, namespace))

  /**
   * Create a Vault provider with mock client for testing.
   *
   * @param secrets Map of path to secret data
   * @return VaultSecretsProvider with mock client
   */
  def withMockClient(secrets: Map[String, String]): VaultSecretsProvider =
    new VaultSecretsProvider(new MockVaultClient(secrets))
}
