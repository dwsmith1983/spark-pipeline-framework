package io.github.dwsmith1983.spark.pipeline.audit

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/** Tests for EnvFilter. */
class EnvFilterSpec extends AnyFunSpec with Matchers {

  val testEnv: Map[String, String] = Map(
    "JAVA_HOME"             -> "/usr/lib/jvm",
    "SPARK_HOME"            -> "/opt/spark",
    "PATH"                  -> "/usr/bin:/bin",
    "USER"                  -> "testuser",
    "AWS_SECRET_ACCESS_KEY" -> "secret123",
    "DATABASE_PASSWORD"     -> "dbpass",
    "MY_API_KEY"            -> "apikey123",
    "CUSTOM_VAR"            -> "custom_value"
  )

  describe("EnvFilter.default") {

    it("should include JAVA_HOME") {
      val result = EnvFilter.default.filter(testEnv)
      (result should contain).key("JAVA_HOME")
    }

    it("should include SPARK_HOME") {
      val result = EnvFilter.default.filter(testEnv)
      (result should contain).key("SPARK_HOME")
    }

    it("should include PATH") {
      val result = EnvFilter.default.filter(testEnv)
      (result should contain).key("PATH")
    }

    it("should include USER") {
      val result = EnvFilter.default.filter(testEnv)
      (result should contain).key("USER")
    }

    it("should exclude AWS_SECRET_ACCESS_KEY") {
      val result = EnvFilter.default.filter(testEnv)
      result should not contain key("AWS_SECRET_ACCESS_KEY")
    }

    it("should exclude DATABASE_PASSWORD") {
      val result = EnvFilter.default.filter(testEnv)
      result should not contain key("DATABASE_PASSWORD")
    }

    it("should exclude CUSTOM_VAR (not in allowlist)") {
      val result = EnvFilter.default.filter(testEnv)
      result should not contain key("CUSTOM_VAR")
    }
  }

  describe("EnvFilter.allowlist") {

    it("should include only specified keys") {
      val filter = EnvFilter.allowlist(Set("JAVA_HOME", "CUSTOM_VAR"))
      val result = filter.filter(testEnv)
      result.keys should contain only ("JAVA_HOME", "CUSTOM_VAR")
    }

    it("should return empty map for non-matching keys") {
      val filter = EnvFilter.allowlist(Set("NON_EXISTENT"))
      val result = filter.filter(testEnv)
      result shouldBe empty
    }
  }

  describe("EnvFilter.withAdditionalAllowedKeys") {

    it("should include defaults plus additional keys") {
      val filter = EnvFilter.withAdditionalAllowedKeys(Set("CUSTOM_VAR"))
      val result = filter.filter(testEnv)
      (result should contain).key("JAVA_HOME")
      (result should contain).key("CUSTOM_VAR")
    }

    it("should still exclude sensitive variables") {
      val filter = EnvFilter.withAdditionalAllowedKeys(Set("CUSTOM_VAR"))
      val result = filter.filter(testEnv)
      result should not contain key("AWS_SECRET_ACCESS_KEY")
    }
  }

  describe("EnvFilter.denylist") {

    it("should exclude keys matching patterns") {
      val filter = EnvFilter.denylist(Set("SECRET", "PASSWORD"))
      val result = filter.filter(testEnv)
      result should not contain key("AWS_SECRET_ACCESS_KEY")
      result should not contain key("DATABASE_PASSWORD")
    }

    it("should include keys not matching patterns") {
      val filter = EnvFilter.denylist(Set("SECRET"))
      val result = filter.filter(testEnv)
      (result should contain).key("JAVA_HOME")
      (result should contain).key("CUSTOM_VAR")
    }

    it("should be case-insensitive") {
      val filter = EnvFilter.denylist(Set("secret"))
      val result = filter.filter(testEnv)
      result should not contain key("AWS_SECRET_ACCESS_KEY")
    }
  }

  describe("EnvFilter.withAdditionalDenyPatterns") {

    it("should exclude defaults plus additional patterns") {
      val filter = EnvFilter.withAdditionalDenyPatterns(Set("CUSTOM"))
      val result = filter.filter(testEnv)
      result should not contain key("AWS_SECRET_ACCESS_KEY")
      result should not contain key("CUSTOM_VAR")
    }

    it("should include safe variables") {
      val filter = EnvFilter.withAdditionalDenyPatterns(Set("CUSTOM"))
      val result = filter.filter(testEnv)
      (result should contain).key("JAVA_HOME")
      (result should contain).key("PATH")
    }
  }

  describe("EnvFilter.empty") {

    it("should return empty map") {
      val result = EnvFilter.empty.filter(testEnv)
      result shouldBe empty
    }
  }

  describe("EnvFilter.passthrough") {

    it("should include all variables") {
      val result = EnvFilter.passthrough.filter(testEnv)
      result shouldBe testEnv
    }
  }

  describe("DefaultAllowKeys") {

    it("should be publicly accessible") {
      EnvFilter.DefaultAllowKeys should contain("JAVA_HOME")
      EnvFilter.DefaultAllowKeys should contain("SPARK_HOME")
      EnvFilter.DefaultAllowKeys should contain("PATH")
    }
  }

  describe("DefaultDenyPatterns") {

    it("should contain all expected patterns") {
      val patterns = EnvFilter.DefaultDenyPatterns
      patterns should contain("PASSWORD")
      patterns should contain("SECRET")
      patterns should contain("KEY")
      patterns should contain("TOKEN")
      patterns should contain("CREDENTIAL")
      patterns should contain("AUTH")
      patterns should contain("PRIVATE")
    }
  }

  describe("edge cases") {

    it("should handle empty environment map") {
      val result = EnvFilter.default.filter(Map.empty)
      result shouldBe empty
    }

    it("should handle environment with only excluded keys") {
      val sensitiveOnly = Map(
        "AWS_SECRET_KEY"    -> "secret",
        "DATABASE_PASSWORD" -> "pass",
        "API_TOKEN"         -> "token"
      )
      val result = EnvFilter.default.filter(sensitiveOnly)
      result shouldBe empty
    }

    it("should handle very long environment values") {
      val longValue = "x" * 10000
      val env       = Map("LONG_VAR" -> longValue)
      val filter    = EnvFilter.allowlist(Set("LONG_VAR"))
      val result    = filter.filter(env)
      result("LONG_VAR") shouldBe longValue
    }

    it("should handle special characters in environment values") {
      val env = Map(
        "SPECIAL_VAR" -> "value with \"quotes\" and \n newlines"
      )
      val filter = EnvFilter.allowlist(Set("SPECIAL_VAR"))
      val result = filter.filter(env)
      result("SPECIAL_VAR") should include("quotes")
      result("SPECIAL_VAR") should include("newlines")
    }

    it("should handle empty string environment values") {
      val env    = Map("EMPTY_VAR" -> "")
      val filter = EnvFilter.allowlist(Set("EMPTY_VAR"))
      val result = filter.filter(env)
      result("EMPTY_VAR") shouldBe ""
    }

    it("should be case-sensitive for key matching in allowlist") {
      val env = Map(
        "java_home" -> "/lower",
        "JAVA_HOME" -> "/upper"
      )
      // Default allowlist has "JAVA_HOME" not "java_home"
      val result = EnvFilter.default.filter(env)
      (result should contain).key("JAVA_HOME")
      result should not contain key("java_home")
    }

    it("should handle overlapping allowlist and denylist patterns") {
      // If a key matches both allowlist and contains deny pattern,
      // the denylist should win (more secure default)
      val filter = EnvFilter.denylist(Set("SECRET"))
      val env    = Map("MY_SECRET_VALUE" -> "hidden")
      val result = filter.filter(env)
      result should not contain key("MY_SECRET_VALUE")
    }
  }
}
