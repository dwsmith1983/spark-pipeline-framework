package io.github.dwsmith1983.spark.pipeline.secrets.providers

import io.github.dwsmith1983.spark.pipeline.secrets.SecretsProvider

import scala.util.{Failure, Success, Try}

/**
 * Secrets provider for AWS Secrets Manager.
 *
 * Supports JSON secrets with key extraction via the #key syntax.
 * Uses the default AWS credential chain (env vars, IAM role, profile, etc.).
 *
 * Usage in config:
 * {{{
 * database {
 *   // Fetch entire secret as string
 *   connection-string = "$${secret:aws://prod/database/connection}"
 *
 *   // Fetch specific key from JSON secret
 *   password = "$${secret:aws://prod/database/credentials#password}"
 * }
 * }}}
 *
 * AWS credential resolution order:
 * 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 * 2. System properties (aws.accessKeyId, aws.secretAccessKey)
 * 3. Web identity token from environment (for EKS/IRSA)
 * 4. Credential profiles file (~/.aws/credentials)
 * 5. EC2 instance profile (for EC2/ECS/Lambda)
 *
 * @param client The AWS Secrets Manager client interface
 */
class AwsSecretsProvider(client: AwsSecretsClient) extends SecretsProvider {

  override val scheme: String = "aws"

  override def resolve(path: String, key: Option[String]): Try[String] =
    client.getSecretValue(path).flatMap { secretValue =>
      key match {
        case None => Success(secretValue)
        case Some(k) => extractJsonKey(secretValue, k, path)
      }
    }

  override def isAvailable: Boolean = client.isAvailable

  override def close(): Unit = client.close()

  private def extractJsonKey(json: String, key: String, path: String): Try[String] =
    Try {
      // Simple JSON key extraction without external dependencies
      // Handles: {"key": "value"} or {"key": 123} or {"key": true}
      val keyPattern = s""""${java.util.regex.Pattern.quote(key)}"\\s*:\\s*"?([^",}]+)"?""".r

      keyPattern.findFirstMatchIn(json) match {
        case Some(m) => m.group(1)
        case None =>
          throw new NoSuchElementException(
            s"Key '$key' not found in secret '$path'. " +
              "Ensure the secret contains valid JSON with the specified key."
          )
      }
    }
}

/**
 * Interface for AWS Secrets Manager operations.
 *
 * Abstracted for testing and to avoid compile-time AWS SDK dependency.
 */
trait AwsSecretsClient {

  /**
   * Get the value of a secret.
   *
   * @param secretId The secret name or ARN
   * @return Success with secret value, Failure on error
   */
  def getSecretValue(secretId: String): Try[String]

  /**
   * Check if the client is properly configured.
   *
   * @return true if credentials are available
   */
  def isAvailable: Boolean

  /**
   * Close the client and release resources.
   */
  def close(): Unit
}

/**
 * AWS Secrets Manager client using AWS SDK v2.
 *
 * Requires aws-sdk-secretsmanager dependency at runtime.
 */
object AwsSecretsManagerClient {

  /**
   * Create a client for the specified region.
   *
   * Requires AWS SDK v2 on classpath:
   * {{{
   * libraryDependencies += "software.amazon.awssdk" % "secretsmanager" % "2.x.x" % Provided
   * }}}
   *
   * @param region AWS region (e.g., "us-east-1")
   * @return Try containing the client or failure if SDK not available
   */
  def create(region: String): Try[AwsSecretsClient] =
    Try {
      // Use reflection to avoid compile-time dependency
      val regionClass = Class.forName("software.amazon.awssdk.regions.Region")
      val regionOf = regionClass.getMethod("of", classOf[String])
      val regionInstance = regionOf.invoke(null, region)

      val builderClass = Class.forName(
        "software.amazon.awssdk.services.secretsmanager.SecretsManagerClient"
      )
      val builderMethod = builderClass.getMethod("builder")
      val builder = builderMethod.invoke(null)

      val regionMethod = builder.getClass.getMethod("region", regionClass)
      regionMethod.invoke(builder, regionInstance)

      val buildMethod = builder.getClass.getMethod("build")
      val client = buildMethod.invoke(builder)

      new ReflectiveAwsClient(client)
    }
}

/**
 * AWS client implementation using reflection.
 *
 * Allows the secrets provider to work without compile-time AWS SDK dependency.
 */
private class ReflectiveAwsClient(client: Any) extends AwsSecretsClient {

  override def getSecretValue(secretId: String): Try[String] =
    Try {
      val requestBuilderClass = Class.forName(
        "software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest"
      )
      val builderMethod = requestBuilderClass.getMethod("builder")
      val builder = builderMethod.invoke(null)

      val secretIdMethod = builder.getClass.getMethod("secretId", classOf[String])
      secretIdMethod.invoke(builder, secretId)

      val buildMethod = builder.getClass.getMethod("build")
      val request = buildMethod.invoke(builder)

      val getSecretMethod = client.getClass.getMethod(
        "getSecretValue",
        requestBuilderClass
      )
      val response = getSecretMethod.invoke(client, request)

      val secretStringMethod = response.getClass.getMethod("secretString")
      val secretString = secretStringMethod.invoke(response)

      if (secretString == null) {
        throw new IllegalStateException(
          s"Secret '$secretId' has no string value (may be binary)"
        )
      }

      secretString.asInstanceOf[String]
    }

  override def isAvailable: Boolean =
    try {
      // Try to describe a non-existent secret to verify credentials
      // This will fail with ResourceNotFoundException if creds are valid
      // or with auth error if not
      val requestBuilderClass = Class.forName(
        "software.amazon.awssdk.services.secretsmanager.model.DescribeSecretRequest"
      )
      val builderMethod = requestBuilderClass.getMethod("builder")
      val builder = builderMethod.invoke(null)

      val secretIdMethod = builder.getClass.getMethod("secretId", classOf[String])
      secretIdMethod.invoke(builder, "__health_check__")

      val buildMethod = builder.getClass.getMethod("build")
      val request = buildMethod.invoke(builder)

      val describeMethod = client.getClass.getMethod("describeSecret", requestBuilderClass)

      try {
        describeMethod.invoke(client, request)
        true
      } catch {
        case e: java.lang.reflect.InvocationTargetException =>
          // ResourceNotFoundException means creds are valid
          e.getCause.getClass.getSimpleName == "ResourceNotFoundException"
      }
    } catch {
      case _: Exception => false
    }

  override def close(): Unit =
    try {
      val closeMethod = client.getClass.getMethod("close")
      val _ = closeMethod.invoke(client)
    } catch {
      case _: Exception => ()
    }
}

/**
 * Mock AWS client for testing.
 *
 * @param secrets Map of secret ID to secret value
 */
class MockAwsSecretsClient(secrets: Map[String, String]) extends AwsSecretsClient {

  override def getSecretValue(secretId: String): Try[String] =
    secrets.get(secretId) match {
      case Some(value) => Success(value)
      case None =>
        Failure(new NoSuchElementException(s"Secret not found: $secretId"))
    }

  override def isAvailable: Boolean = true

  override def close(): Unit = ()
}

object AwsSecretsProvider {

  /**
   * Create an AWS provider for the specified region.
   *
   * @param region AWS region
   * @return Try containing the provider or failure if AWS SDK not available
   */
  def apply(region: String): Try[AwsSecretsProvider] =
    AwsSecretsManagerClient.create(region).map(new AwsSecretsProvider(_))

  /**
   * Create an AWS provider with a mock client for testing.
   *
   * @param secrets Map of secret ID to secret value
   * @return AwsSecretsProvider with mock client
   */
  def withMockClient(secrets: Map[String, String]): AwsSecretsProvider =
    new AwsSecretsProvider(new MockAwsSecretsClient(secrets))
}
