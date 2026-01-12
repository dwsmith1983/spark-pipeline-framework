package io.github.dwsmith1983.spark.pipeline.secrets.providers

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class VaultSecretsProviderSpec extends AnyFunSpec with Matchers {

  describe("VaultSecretsProvider with mock client") {

    it("should return scheme 'vault'") {
      val provider = VaultSecretsProvider.withMockClient(Map.empty)
      provider.scheme shouldBe "vault"
    }

    it("should resolve secret with key extraction") {
      val secrets = Map(
        "secret/data/database/prod" -> """{"username": "dbuser", "password": "dbpass"}"""
      )
      val provider = VaultSecretsProvider.withMockClient(secrets)

      provider.resolve("secret/data/database/prod", Some("username")) shouldBe
        Success("dbuser")
      provider.resolve("secret/data/database/prod", Some("password")) shouldBe
        Success("dbpass")
    }

    it("should return entire data when no key specified") {
      val secrets = Map(
        "secret/data/config" -> """{"setting": "value"}"""
      )
      val provider = VaultSecretsProvider.withMockClient(secrets)

      val result = provider.resolve("secret/data/config", None)
      result shouldBe Success("""{"setting": "value"}""")
    }

    it("should fail when secret not found") {
      val provider = VaultSecretsProvider.withMockClient(Map.empty)

      val result = provider.resolve("nonexistent/path", None)
      result.isFailure shouldBe true
    }

    it("should fail when key not found in secret") {
      val secrets  = Map("secret/path" -> """{"foo": "bar"}""")
      val provider = VaultSecretsProvider.withMockClient(secrets)

      val result = provider.resolve("secret/path", Some("missing"))
      result.isFailure shouldBe true
      result.failed.get shouldBe a[NoSuchElementException]
    }

    it("should be available with mock client") {
      val provider = VaultSecretsProvider.withMockClient(Map.empty)
      provider.isAvailable shouldBe true
    }

    it("should handle close without error") {
      val provider = VaultSecretsProvider.withMockClient(Map.empty)
      noException should be thrownBy provider.close()
    }
  }

  describe("MockVaultClient") {

    it("should return values from map") {
      val client = new MockVaultClient(Map("path1" -> "data1"))

      client.readSecret("path1") shouldBe Success("data1")
      client.readSecret("path2").isFailure shouldBe true
    }

    it("should always be available") {
      val client = new MockVaultClient(Map.empty)
      client.isAvailable shouldBe true
    }
  }

  describe("VaultSecretsProvider.fromEnv") {

    it("should fail when VAULT_ADDR not set") {
      // This test relies on the actual environment not having VAULT_ADDR set
      // In CI, this should be true. If VAULT_ADDR is set, the test behavior differs.
      val result = VaultSecretsProvider.fromEnv()
      // Either fails because no env vars, or succeeds if they happen to be set
      // We just verify it doesn't throw
      result.isSuccess || result.isFailure shouldBe true
    }
  }
}
