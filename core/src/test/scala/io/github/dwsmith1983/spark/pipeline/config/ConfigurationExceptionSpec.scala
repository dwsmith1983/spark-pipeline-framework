package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import pureconfig._
import pureconfig.error.ConfigReaderException
import pureconfig.generic.auto._

/** Tests for ConfigurationException. */
class ConfigurationExceptionSpec extends AnyFunSpec with Matchers {

  case class TestConfig(requiredField: String, count: Int)

  describe("ConfigurationException") {

    it("should store message and null cause when created directly") {
      val exception = new ConfigurationException("Invalid configuration")

      exception.getMessage shouldBe "Invalid configuration"
      exception.getCause shouldBe null
    }

    it("should store message and cause when provided") {
      val cause     = new RuntimeException("underlying error")
      val exception = new ConfigurationException("Config failed", cause)

      exception.getMessage shouldBe "Config failed"
      exception.getCause shouldBe cause
    }

    it("should extend RuntimeException") {
      val exception = new ConfigurationException("test")

      exception shouldBe a[RuntimeException]
    }
  }

  describe("ConfigurationException.fromConfigReaderException") {

    it("should extract prettyPrint message from ConfigReaderException") {
      val invalidConfig = ConfigFactory.parseString("")

      val configReaderException = intercept[ConfigReaderException[TestConfig]] {
        ConfigSource.fromConfig(invalidConfig).loadOrThrow[TestConfig]
      }

      val wrapped = ConfigurationException.fromConfigReaderException(configReaderException)

      // PureConfig uses kebab-case by default
      wrapped.getMessage should include("required-field")
      wrapped.getMessage should include("count")
    }

    it("should preserve original ConfigReaderException as cause") {
      val invalidConfig = ConfigFactory.parseString("count = \"not-an-int\"")

      val configReaderException = intercept[ConfigReaderException[TestConfig]] {
        ConfigSource.fromConfig(invalidConfig).loadOrThrow[TestConfig]
      }

      val wrapped = ConfigurationException.fromConfigReaderException(configReaderException)

      wrapped.getCause shouldBe configReaderException
    }

    it("should include field path in error message for type mismatches") {
      val invalidConfig = ConfigFactory.parseString("""
        requiredField = "valid"
        count = "not-a-number"
      """)

      val configReaderException = intercept[ConfigReaderException[TestConfig]] {
        ConfigSource.fromConfig(invalidConfig).loadOrThrow[TestConfig]
      }

      val wrapped = ConfigurationException.fromConfigReaderException(configReaderException)

      wrapped.getMessage should include("count")
    }
  }
}
