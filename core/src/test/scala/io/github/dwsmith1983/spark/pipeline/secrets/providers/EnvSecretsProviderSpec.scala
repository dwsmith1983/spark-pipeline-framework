package io.github.dwsmith1983.spark.pipeline.secrets.providers

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class EnvSecretsProviderSpec extends AnyFunSpec with Matchers {

  describe("EnvSecretsProvider") {

    it("should return scheme 'env'") {
      val provider = EnvSecretsProvider()
      provider.scheme shouldBe "env"
    }

    it("should resolve existing environment variables") {
      val env      = Map("TEST_SECRET" -> "secret-value")
      val provider = EnvSecretsProvider.withEnv(env)

      provider.resolve("TEST_SECRET", None) shouldBe Success("secret-value")
    }

    it("should fail for missing environment variables") {
      val provider = EnvSecretsProvider.withEnv(Map.empty)

      val result = provider.resolve("NONEXISTENT_VAR", None)
      result.isFailure shouldBe true
      result.failed.get shouldBe a[NoSuchElementException]
    }

    it("should reject key syntax") {
      val env      = Map("MY_SECRET" -> """{"key": "value"}""")
      val provider = EnvSecretsProvider.withEnv(env)

      val result = provider.resolve("MY_SECRET", Some("key"))
      result.isFailure shouldBe true
      result.failed.get shouldBe a[IllegalArgumentException]
    }

    it("should always be available") {
      val provider = EnvSecretsProvider()
      provider.isAvailable shouldBe true
    }

    it("should handle close without error") {
      val provider = EnvSecretsProvider()
      noException should be thrownBy provider.close()
    }
  }

  describe("EnvSecretsProvider.withEnv") {

    it("should use custom environment map") {
      val customEnv = Map(
        "DB_HOST" -> "localhost",
        "DB_PORT" -> "5432",
        "DB_PASS" -> "secret123"
      )
      val provider = EnvSecretsProvider.withEnv(customEnv)

      provider.resolve("DB_HOST", None) shouldBe Success("localhost")
      provider.resolve("DB_PORT", None) shouldBe Success("5432")
      provider.resolve("DB_PASS", None) shouldBe Success("secret123")
      provider.resolve("DB_USER", None).isFailure shouldBe true
    }
  }
}
