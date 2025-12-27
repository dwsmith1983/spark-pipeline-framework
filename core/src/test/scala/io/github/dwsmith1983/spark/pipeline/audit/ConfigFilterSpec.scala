package io.github.dwsmith1983.spark.pipeline.audit

import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Tests for ConfigFilter. */
class ConfigFilterSpec extends AnyFunSpec with Matchers {

  describe("ConfigFilter.default") {

    it("should redact password keys") {
      val config = ConfigFactory.parseString("db.password = secret123")
      val result = ConfigFilter.default.filter(config)
      result("db.password") shouldBe "***REDACTED***"
    }

    it("should redact secret keys") {
      val config = ConfigFactory.parseString("api.secret = abc123")
      val result = ConfigFilter.default.filter(config)
      result("api.secret") shouldBe "***REDACTED***"
    }

    it("should redact token keys") {
      val config = ConfigFactory.parseString("auth.token = xyz789")
      val result = ConfigFilter.default.filter(config)
      result("auth.token") shouldBe "***REDACTED***"
    }

    it("should redact api_key keys") {
      val config = ConfigFactory.parseString("service.api_key = key123")
      val result = ConfigFilter.default.filter(config)
      result("service.api_key") shouldBe "***REDACTED***"
    }

    it("should pass through safe keys") {
      val config = ConfigFactory.parseString("""app.name = "MyApp", app.version = "1.0"""")
      val result = ConfigFilter.default.filter(config)
      result("app.name") should include("MyApp")
      result("app.version") should include("1.0")
    }

    it("should be case-insensitive") {
      val config = ConfigFactory.parseString("db.PASSWORD = secret, API_KEY = key")
      val result = ConfigFilter.default.filter(config)
      result("db.PASSWORD") shouldBe "***REDACTED***"
      result("API_KEY") shouldBe "***REDACTED***"
    }
  }

  describe("ConfigFilter.withPatterns") {

    it("should redact custom patterns") {
      val filter = ConfigFilter.withPatterns(Set("mycompany_secret"))
      val config = ConfigFactory.parseString("mycompany_secret_key = abc123")
      val result = filter.filter(config)
      result("mycompany_secret_key") shouldBe "***REDACTED***"
    }

    it("should not redact default patterns if not specified") {
      val filter = ConfigFilter.withPatterns(Set("custom"))
      val config = ConfigFactory.parseString("password = secret, custom_key = value")
      val result = filter.filter(config)
      result("password") should include("secret") // Not redacted
      result("custom_key") shouldBe "***REDACTED***"
    }
  }

  describe("ConfigFilter.withAdditionalPatterns") {

    it("should redact both defaults and additional patterns") {
      val filter = ConfigFilter.withAdditionalPatterns(Set("internal"))
      val config = ConfigFactory.parseString("password = secret, internal_key = value")
      val result = filter.filter(config)
      result("password") shouldBe "***REDACTED***"
      result("internal_key") shouldBe "***REDACTED***"
    }

    it("should still pass through safe keys") {
      val filter = ConfigFilter.withAdditionalPatterns(Set("internal"))
      val config = ConfigFactory.parseString("app.name = MyApp")
      val result = filter.filter(config)
      result("app.name") should include("MyApp")
    }
  }

  describe("ConfigFilter.passthrough") {

    it("should not redact any values") {
      val config = ConfigFactory.parseString("password = secret, token = abc")
      val result = ConfigFilter.passthrough.filter(config)
      result("password") should include("secret")
      result("token") should include("abc")
    }
  }

  describe("DefaultSensitivePatterns") {

    it("should contain all expected patterns") {
      val patterns = ConfigFilter.DefaultSensitivePatterns
      patterns should contain("password")
      patterns should contain("secret")
      patterns should contain("key")
      patterns should contain("token")
      patterns should contain("credential")
      patterns should contain("auth")
      patterns should contain("private")
      patterns should contain("apikey")
      patterns should contain("api_key")
      patterns should contain("api-key")
      patterns should have size 10
    }
  }

  describe("edge cases") {

    it("should handle empty config") {
      val config = ConfigFactory.empty()
      val result = ConfigFilter.default.filter(config)
      result shouldBe empty
    }

    it("should handle config with only sensitive keys") {
      val config = ConfigFactory.parseString("password = secret, token = abc, api_key = xyz")
      val result = ConfigFilter.default.filter(config)
      result.values.foreach { value =>
        value shouldBe "***REDACTED***"
      }
    }

    it("should handle nested config paths with sensitive names") {
      val config = ConfigFactory.parseString("""
        |db {
        |  connection {
        |    password = "secret123"
        |    url = "jdbc:postgres://localhost"
        |  }
        |}
        |""".stripMargin)
      val result = ConfigFilter.default.filter(config)
      result("db.connection.password") shouldBe "***REDACTED***"
      result("db.connection.url") should include("jdbc:postgres")
    }

    it("should handle keys that partially match patterns") {
      val config = ConfigFactory.parseString("""
        |password_hash = "abc123"
        |my_secret_value = "hidden"
        |token_expiry = "3600"
        |""".stripMargin)
      val result = ConfigFilter.default.filter(config)
      // All should be redacted because they contain sensitive patterns
      result("password_hash") shouldBe "***REDACTED***"
      result("my_secret_value") shouldBe "***REDACTED***"
      result("token_expiry") shouldBe "***REDACTED***"
    }

    it("should handle very long config values") {
      val longValue = "x" * 10000
      val config    = ConfigFactory.parseString(s"""data = "$longValue"""")
      val result    = ConfigFilter.default.filter(config)
      result("data") should include(longValue)
    }

    it("should handle special characters in values") {
      val config = ConfigFactory.parseString("""
        |message = "Hello \"World\" with \n newlines"
        |path = "C:\\Users\\test"
        |""".stripMargin)
      val result = ConfigFilter.default.filter(config)
      result should contain key "message"
      result should contain key "path"
    }

    it("should handle numeric and boolean values") {
      val config = ConfigFactory.parseString("""
        |count = 42
        |enabled = true
        |ratio = 3.14
        |""".stripMargin)
      val result = ConfigFilter.default.filter(config)
      result("count") should include("42")
      result("enabled") should include("true")
      result("ratio") should include("3.14")
    }
  }
}
