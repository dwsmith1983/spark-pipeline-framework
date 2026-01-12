package io.github.dwsmith1983.spark.pipeline.secrets

import io.github.dwsmith1983.spark.pipeline.secrets.audit.{InMemorySecretsAuditLogger, SecretsAuditLogger}
import io.github.dwsmith1983.spark.pipeline.secrets.providers.{
  AwsSecretsProvider,
  EnvSecretsProvider,
  VaultSecretsProvider
}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class SecretsResolverSpec extends AnyFunSpec with Matchers {

  describe("SecretsResolver.resolve") {

    it("should return config unchanged when no references") {
      val resolver = SecretsResolver.builder().build()
      val config   = """database { host = "localhost" }"""

      resolver.resolve(config) shouldBe Success(config)
    }

    it("should resolve environment variable references") {
      val env = Map("DB_PASSWORD" -> "secret123")
      val resolver = SecretsResolver.builder()
        .withProvider(EnvSecretsProvider.withEnv(env))
        .build()

      val config   = """database { password = "${secret:env://DB_PASSWORD}" }"""
      val expected = """database { password = "secret123" }"""

      resolver.resolve(config) shouldBe Success(expected)
    }

    it("should resolve multiple references") {
      val env = Map(
        "DB_USER" -> "admin",
        "DB_PASS" -> "secret"
      )
      val resolver = SecretsResolver.builder()
        .withProvider(EnvSecretsProvider.withEnv(env))
        .build()

      val config =
        """database {
          |  username = "${secret:env://DB_USER}"
          |  password = "${secret:env://DB_PASS}"
          |}""".stripMargin

      val result = resolver.resolve(config)
      result.isSuccess shouldBe true
      result.get should include("admin")
      result.get should include("secret")
    }

    it("should resolve AWS secrets with key extraction") {
      val awsSecrets = Map(
        "prod/database" -> """{"username": "dbadmin", "password": "dbsecret"}"""
      )
      val resolver = SecretsResolver.builder()
        .withProvider(AwsSecretsProvider.withMockClient(awsSecrets))
        .build()

      val config =
        """database {
          |  username = "${secret:aws://prod/database#username}"
          |  password = "${secret:aws://prod/database#password}"
          |}""".stripMargin

      val result = resolver.resolve(config)
      result.isSuccess shouldBe true
      result.get should include("dbadmin")
      result.get should include("dbsecret")
    }

    it("should resolve mixed provider references") {
      val env          = Map("API_KEY" -> "key123")
      val vaultSecrets = Map("secret/db" -> """{"password": "vaultpass"}""")

      val resolver = SecretsResolver.builder()
        .withProvider(EnvSecretsProvider.withEnv(env))
        .withProvider(VaultSecretsProvider.withMockClient(vaultSecrets))
        .build()

      val config =
        """
          |api-key = "${secret:env://API_KEY}"
          |db-pass = "${secret:vault://secret/db#password}"
          |""".stripMargin

      val result = resolver.resolve(config)
      result.isSuccess shouldBe true
      result.get should include("key123")
      result.get should include("vaultpass")
    }

    it("should fail when provider not available") {
      val resolver = SecretsResolver.builder()
        .withProvider(EnvSecretsProvider.withEnv(Map.empty))
        .build()

      val config = """password = "${secret:aws://some/secret}""""

      val result = resolver.resolve(config)
      result.isFailure shouldBe true
      result.failed.get shouldBe a[SecretResolutionException]
    }

    it("should fail when secret not found") {
      val resolver = SecretsResolver.builder()
        .withProvider(EnvSecretsProvider.withEnv(Map.empty))
        .build()

      val config = """password = "${secret:env://MISSING_VAR}""""

      val result = resolver.resolve(config)
      result.isFailure shouldBe true
    }

    it("should use cache for duplicate references") {
      var resolveCount = 0
      val customProvider = new SecretsProvider {
        override def scheme: String = "test"
        override def resolve(path: String, key: Option[String]): scala.util.Try[String] = {
          resolveCount += 1
          Success("resolved")
        }
        override def isAvailable: Boolean = true
        override def close(): Unit        = ()
      }

      val resolver = SecretsResolver.builder()
        .withProvider(customProvider)
        .build()

      val config =
        """
          |a = "${secret:test://same-path}"
          |b = "${secret:test://same-path}"
          |""".stripMargin

      resolver.resolve(config)

      // Should only resolve once due to caching
      resolveCount shouldBe 1
    }
  }

  describe("SecretsResolver.resolveAndParse") {

    it("should resolve and parse as Config") {
      val env = Map("DB_HOST" -> "localhost")
      val resolver = SecretsResolver.builder()
        .withProvider(EnvSecretsProvider.withEnv(env))
        .build()

      val configStr = """database { host = "${secret:env://DB_HOST}" }"""

      val result = resolver.resolveAndParse(configStr)
      result.isSuccess shouldBe true
      result.get.getString("database.host") shouldBe "localhost"
    }
  }

  describe("SecretsResolver with audit logging") {

    it("should log successful access events") {
      val auditLogger = SecretsAuditLogger.inMemory
      val env         = Map("SECRET" -> "value")

      val resolver = SecretsResolver.builder()
        .withProvider(EnvSecretsProvider.withEnv(env))
        .withAuditLogger(auditLogger)
        .build()

      val config = """x = "${secret:env://SECRET}""""
      resolver.resolve(config)

      val events = auditLogger.getEvents
      events should have size 1
      events.head.success shouldBe true
      events.head.provider shouldBe "env"
      events.head.path shouldBe "SECRET"
    }

    it("should log failed access events") {
      val auditLogger = SecretsAuditLogger.inMemory

      val resolver = SecretsResolver.builder()
        .withProvider(EnvSecretsProvider.withEnv(Map.empty))
        .withAuditLogger(auditLogger)
        .build()

      val config = """x = "${secret:env://MISSING}""""
      resolver.resolve(config)

      val events = auditLogger.getEvents
      events should have size 1
      events.head.success shouldBe false
      events.head.errorMessage shouldBe defined
    }
  }

  describe("SecretsResolver.checkProviders") {

    it("should return availability status of all providers") {
      val resolver = SecretsResolver.builder()
        .withProvider(EnvSecretsProvider())
        .withProvider(VaultSecretsProvider.withMockClient(Map.empty))
        .build()

      val status = resolver.checkProviders()

      (status should contain).key("env")
      (status should contain).key("vault")
      status("env") shouldBe true
      status("vault") shouldBe true
    }
  }

  describe("SecretsResolver.availableSchemes") {

    it("should return sorted list of schemes") {
      val resolver = SecretsResolver.builder()
        .withProvider(VaultSecretsProvider.withMockClient(Map.empty))
        .withProvider(EnvSecretsProvider())
        .build()

      resolver.availableSchemes shouldBe Seq("env", "vault")
    }
  }

  describe("SecretsResolver.withEnvProvider") {

    it("should create resolver with env provider") {
      val resolver = SecretsResolver.withEnvProvider()
      resolver.availableSchemes should contain("env")
    }
  }
}
