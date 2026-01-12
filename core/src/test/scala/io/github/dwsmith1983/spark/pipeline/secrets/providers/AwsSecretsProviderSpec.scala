package io.github.dwsmith1983.spark.pipeline.secrets.providers

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class AwsSecretsProviderSpec extends AnyFunSpec with Matchers {

  describe("AwsSecretsProvider with mock client") {

    it("should return scheme 'aws'") {
      val provider = AwsSecretsProvider.withMockClient(Map.empty)
      provider.scheme shouldBe "aws"
    }

    it("should resolve secret without key") {
      val secrets  = Map("my-app/connection" -> "postgresql://localhost:5432/db")
      val provider = AwsSecretsProvider.withMockClient(secrets)

      provider.resolve("my-app/connection", None) shouldBe
        Success("postgresql://localhost:5432/db")
    }

    it("should resolve secret with JSON key extraction") {
      val secrets = Map(
        "my-app/credentials" -> """{"username": "admin", "password": "secret123"}"""
      )
      val provider = AwsSecretsProvider.withMockClient(secrets)

      provider.resolve("my-app/credentials", Some("username")) shouldBe Success("admin")
      provider.resolve("my-app/credentials", Some("password")) shouldBe Success("secret123")
    }

    it("should fail when secret not found") {
      val provider = AwsSecretsProvider.withMockClient(Map.empty)

      val result = provider.resolve("nonexistent", None)
      result.isFailure shouldBe true
    }

    it("should fail when JSON key not found") {
      val secrets  = Map("my-secret" -> """{"foo": "bar"}""")
      val provider = AwsSecretsProvider.withMockClient(secrets)

      val result = provider.resolve("my-secret", Some("missing-key"))
      result.isFailure shouldBe true
      result.failed.get shouldBe a[NoSuchElementException]
    }

    it("should be available with mock client") {
      val provider = AwsSecretsProvider.withMockClient(Map.empty)
      provider.isAvailable shouldBe true
    }

    it("should handle close without error") {
      val provider = AwsSecretsProvider.withMockClient(Map.empty)
      noException should be thrownBy provider.close()
    }

    it("should extract numeric values from JSON") {
      val secrets  = Map("config" -> """{"port": 5432, "enabled": true}""")
      val provider = AwsSecretsProvider.withMockClient(secrets)

      provider.resolve("config", Some("port")) shouldBe Success("5432")
      provider.resolve("config", Some("enabled")) shouldBe Success("true")
    }
  }

  describe("MockAwsSecretsClient") {

    it("should return values from map") {
      val client = new MockAwsSecretsClient(Map("secret1" -> "value1"))

      client.getSecretValue("secret1") shouldBe Success("value1")
      client.getSecretValue("secret2").isFailure shouldBe true
    }

    it("should always be available") {
      val client = new MockAwsSecretsClient(Map.empty)
      client.isAvailable shouldBe true
    }
  }
}
