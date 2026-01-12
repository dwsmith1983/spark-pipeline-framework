# Secrets Management

Secure credential handling for Spark pipelines. Secrets are resolved at runtime from external secret stores, never stored in configuration files.

## Quick Start

Use secret references in your HOCON configuration:

```hocon
spark {
  config {
    "spark.hadoop.fs.s3a.access.key" = "${secret:env://AWS_ACCESS_KEY_ID}"
    "spark.hadoop.fs.s3a.secret.key" = "${secret:env://AWS_SECRET_ACCESS_KEY}"
  }
}

pipeline {
  pipeline-components = [
    {
      instance-type = "com.example.JdbcSource"
      instance-config {
        jdbc-url = "jdbc:postgresql://prod-db:5432/mydb"
        username = "${secret:aws://prod/database#username}"
        password = "${secret:aws://prod/database#password}"
      }
    }
  ]
}
```

## Reference Format

```
${secret:<provider>://<path>#<key>}
```

| Component | Description |
|-----------|-------------|
| `provider` | The secrets backend: `env`, `aws`, `vault` |
| `path` | Path to the secret in the provider |
| `key` | Optional: specific key within a JSON secret |

## Supported Providers

### Environment Variables (`env://`)

Reads secrets from environment variables. Best for local development and CI/CD.

```hocon
database.password = "${secret:env://DB_PASSWORD}"
```

No additional setup required.

### AWS Secrets Manager (`aws://`)

Reads secrets from AWS Secrets Manager. Supports JSON secrets with key extraction.

```hocon
# Entire secret as string
connection = "${secret:aws://prod/connection-string}"

# Specific key from JSON secret
password = "${secret:aws://prod/database#password}"
```

**Setup:**
1. Add AWS SDK dependency to your project:
   ```scala
   libraryDependencies += "software.amazon.awssdk" % "secretsmanager" % "2.29.51"
   ```

2. Configure AWS credentials via:
   - Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
   - IAM role (EC2, ECS, Lambda)
   - AWS profile (~/.aws/credentials)

### HashiCorp Vault (`vault://`)

Reads secrets from HashiCorp Vault KV secrets engine (v2).

```hocon
# Specific key from secret
password = "${secret:vault://secret/data/database/prod#password}"
```

**Setup:**
1. Set environment variables:
   - `VAULT_ADDR`: Vault server address (e.g., `https://vault.example.com:8200`)
   - `VAULT_TOKEN`: Authentication token
   - `VAULT_NAMESPACE` (optional): Vault Enterprise namespace

## Programmatic Usage

### Basic Usage

```scala
import io.github.dwsmith1983.spark.pipeline.secrets._
import io.github.dwsmith1983.spark.pipeline.secrets.providers._

// Create resolver with providers
val resolver = SecretsResolver.builder()
  .withProvider(EnvSecretsProvider())
  .withProvider(AwsSecretsProvider("us-east-1").get)
  .build()

// Resolve secrets in config string
val configWithSecrets = """password = "${secret:env://DB_PASS}""""
val resolvedConfig = resolver.resolve(configWithSecrets)

// Or resolve and parse as Config
val config = resolver.resolveAndParse(configWithSecrets)
```

### With Caching

```scala
val resolver = SecretsResolver.builder()
  .withProvider(EnvSecretsProvider())
  .withCache(SecretsCache.withTtlSeconds(300)) // 5 minute TTL
  .build()
```

### With Audit Logging

```scala
import io.github.dwsmith1983.spark.pipeline.secrets.audit._

val resolver = SecretsResolver.builder()
  .withProvider(EnvSecretsProvider())
  .withAuditLogger(SecretsAuditLogger.default) // Log4j2
  .build()
```

## Security Best Practices

### Do

- Use IAM roles for AWS credentials in production (EC2/ECS/Lambda)
- Use Vault AppRole or Kubernetes auth in production
- Set appropriate TTL for cached secrets (default: 5 minutes)
- Enable audit logging in production
- Use environment variables for local development

### Don't

- Never commit secrets to version control
- Never log secret values (audit logger only logs paths)
- Avoid hardcoding secrets in configuration
- Don't disable caching in high-throughput scenarios

### Credential Provider Priority

**AWS:**
1. Environment variables
2. System properties
3. Web identity token (EKS/IRSA)
4. Credential profiles file
5. EC2/ECS instance profile

**Vault:**
1. VAULT_TOKEN environment variable
2. Token file (~/.vault-token)
3. AppRole authentication (configure programmatically)

## Extending with Custom Providers

Implement the `SecretsProvider` trait:

```scala
import io.github.dwsmith1983.spark.pipeline.secrets.SecretsProvider
import scala.util.Try

class MySecretsProvider extends SecretsProvider {
  override def scheme: String = "my"

  override def resolve(path: String, key: Option[String]): Try[String] = {
    // Fetch secret from your backend
    Try {
      fetchFromMyBackend(path, key)
    }
  }

  override def isAvailable: Boolean = {
    // Check if provider is configured
    true
  }

  override def close(): Unit = {
    // Clean up resources
  }
}

// Use it
val resolver = SecretsResolver.builder()
  .withProvider(new MySecretsProvider)
  .build()
```

## Error Handling

Secrets resolution fails fast. If any secret cannot be resolved, the entire configuration resolution fails with a `SecretResolutionException`.

```scala
resolver.resolve(config) match {
  case Success(resolved) =>
    // Use resolved config
  case Failure(e: SecretResolutionException) =>
    // Handle specific secret resolution failure
    logger.error(s"Failed to resolve: ${e.reference.original}", e)
  case Failure(e) =>
    // Handle other errors
}
```

## Testing

Use mock providers for testing:

```scala
// Mock AWS provider
val awsProvider = AwsSecretsProvider.withMockClient(Map(
  "test/secret" -> """{"username": "test", "password": "test123"}"""
))

// Mock Vault provider
val vaultProvider = VaultSecretsProvider.withMockClient(Map(
  "secret/data/test" -> """{"api_key": "key123"}"""
))

// Custom environment
val envProvider = EnvSecretsProvider.withEnv(Map(
  "TEST_VAR" -> "test-value"
))

val resolver = SecretsResolver.builder()
  .withProvider(awsProvider)
  .withProvider(vaultProvider)
  .withProvider(envProvider)
  .build()
```
