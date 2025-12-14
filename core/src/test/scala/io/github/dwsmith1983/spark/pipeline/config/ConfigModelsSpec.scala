package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import pureconfig._
import pureconfig.generic.auto._

/**
 * Tests for config model parsing with PureConfig.
 *
 * Verifies that HOCON configs correctly parse into case classes
 * with proper kebab-case to camelCase conversion.
 */
class ConfigModelsSpec extends AnyFunSpec with Matchers {

  describe("PipelineConfig") {

    describe("happy path") {

      it("should parse a complete pipeline config") {
        val hocon = ConfigFactory.parseString("""
          pipeline-name = "Test Pipeline"
          pipeline-components = [
            {
              instance-type = "com.example.MyComponent"
              instance-name = "MyComponent(test)"
              instance-config {
                some-key = "some-value"
              }
            }
          ]
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[PipelineConfig]

        config.pipelineName shouldBe "Test Pipeline"
        config.pipelineComponents should have size 1
        config.pipelineComponents.head.instanceType shouldBe "com.example.MyComponent"
        config.pipelineComponents.head.instanceName shouldBe "MyComponent(test)"
      }

      it("should parse multiple components") {
        val hocon = ConfigFactory.parseString("""
          pipeline-name = "Multi-Component Pipeline"
          pipeline-components = [
            {
              instance-type = "com.example.First"
              instance-name = "First"
              instance-config {}
            },
            {
              instance-type = "com.example.Second"
              instance-name = "Second"
              instance-config {}
            },
            {
              instance-type = "com.example.Third"
              instance-name = "Third"
              instance-config {}
            }
          ]
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[PipelineConfig]

        config.pipelineComponents should have size 3
        config.pipelineComponents.map(_.instanceType) shouldBe List(
          "com.example.First",
          "com.example.Second",
          "com.example.Third"
        )
      }
    }

    describe("unhappy path") {

      it("should fail when pipeline-name is missing") {
        val hocon = ConfigFactory.parseString("""
          pipeline-components = []
        """)

        val result = ConfigSource.fromConfig(hocon).load[PipelineConfig]

        result.isLeft shouldBe true
      }

      it("should fail when pipeline-components is missing") {
        val hocon = ConfigFactory.parseString("""
          pipeline-name = "Test"
        """)

        val result = ConfigSource.fromConfig(hocon).load[PipelineConfig]

        result.isLeft shouldBe true
      }
    }

    describe("edge cases") {

      it("should handle empty pipeline-components list") {
        val hocon = ConfigFactory.parseString("""
          pipeline-name = "Empty Pipeline"
          pipeline-components = []
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[PipelineConfig]

        config.pipelineName shouldBe "Empty Pipeline"
        config.pipelineComponents shouldBe empty
      }

      it("should handle special characters in pipeline-name") {
        val hocon = ConfigFactory.parseString("""
          pipeline-name = "Pipeline with spaces & special-chars!"
          pipeline-components = []
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[PipelineConfig]

        config.pipelineName shouldBe "Pipeline with spaces & special-chars!"
      }
    }
  }

  describe("ComponentConfig") {

    describe("happy path") {

      it("should preserve raw instance-config for downstream parsing") {
        val hocon = ConfigFactory.parseString("""
          instance-type = "com.example.Component"
          instance-name = "Component(test)"
          instance-config {
            nested-key = "nested-value"
            numeric-key = 123
            list-key = [1, 2, 3]
          }
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[ComponentConfig]

        config.instanceConfig.getString("nested-key") shouldBe "nested-value"
        config.instanceConfig.getInt("numeric-key") shouldBe 123
        (config.instanceConfig.getIntList("list-key") should contain).allOf(1, 2, 3)
      }
    }

    describe("unhappy path") {

      it("should fail when instance-type is missing") {
        val hocon = ConfigFactory.parseString("""
          instance-name = "Test"
          instance-config {}
        """)

        val result = ConfigSource.fromConfig(hocon).load[ComponentConfig]

        result.isLeft shouldBe true
      }

      it("should fail when instance-name is missing") {
        val hocon = ConfigFactory.parseString("""
          instance-type = "com.example.Test"
          instance-config {}
        """)

        val result = ConfigSource.fromConfig(hocon).load[ComponentConfig]

        result.isLeft shouldBe true
      }

      it("should fail when instance-config is missing") {
        val hocon = ConfigFactory.parseString("""
          instance-type = "com.example.Test"
          instance-name = "Test"
        """)

        val result = ConfigSource.fromConfig(hocon).load[ComponentConfig]

        result.isLeft shouldBe true
      }
    }

    describe("edge cases") {

      it("should handle empty instance-config") {
        val hocon = ConfigFactory.parseString("""
          instance-type = "com.example.Component"
          instance-name = "Component(empty)"
          instance-config {}
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[ComponentConfig]

        config.instanceConfig.isEmpty shouldBe true
      }

      it("should handle deeply nested instance-config") {
        val hocon = ConfigFactory.parseString("""
          instance-type = "com.example.Component"
          instance-name = "Component(nested)"
          instance-config {
            level1 {
              level2 {
                level3 {
                  deep-value = "found"
                }
              }
            }
          }
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[ComponentConfig]

        config.instanceConfig.getString("level1.level2.level3.deep-value") shouldBe "found"
      }
    }
  }

  describe("SparkConfig") {

    describe("happy path") {

      it("should parse complete Spark config") {
        val hocon = ConfigFactory.parseString("""
          master = "yarn"
          app-name = "TestApp"
          config {
            "spark.executor.memory" = "4g"
            "spark.executor.cores" = "2"
          }
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[SparkConfig]

        config.master shouldBe Some("yarn")
        config.appName shouldBe Some("TestApp")
        config.config should contain("spark.executor.memory" -> "4g")
        config.config should contain("spark.executor.cores" -> "2")
      }

      it("should use defaults when optional fields not provided") {
        val hocon = ConfigFactory.parseString("""
          config {}
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[SparkConfig]

        config.master shouldBe None
        config.appName shouldBe None
        config.config shouldBe empty
      }
    }

    describe("edge cases") {

      it("should handle empty config block") {
        val hocon = ConfigFactory.parseString("""
          master = "local[*]"
          app-name = "Test"
          config {}
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[SparkConfig]

        config.config shouldBe empty
      }

      it("should handle many Spark config entries") {
        val hocon = ConfigFactory.parseString("""
          master = "yarn"
          app-name = "Test"
          config {
            "spark.executor.memory" = "4g"
            "spark.executor.cores" = "2"
            "spark.driver.memory" = "2g"
            "spark.dynamicAllocation.enabled" = "true"
            "spark.sql.adaptive.enabled" = "true"
            "spark.sql.shuffle.partitions" = "200"
            "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"
          }
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[SparkConfig]

        config.config should have size 7
      }

      it("should handle Spark config with dots in keys") {
        val hocon = ConfigFactory.parseString("""
          config {
            "spark.hadoop.fs.s3a.endpoint" = "s3.amazonaws.com"
            "spark.hadoop.fs.s3a.access.key" = "xxx"
          }
        """)

        val config = ConfigSource.fromConfig(hocon).loadOrThrow[SparkConfig]

        (config.config should contain).key("spark.hadoop.fs.s3a.endpoint")
      }
    }
  }
}
