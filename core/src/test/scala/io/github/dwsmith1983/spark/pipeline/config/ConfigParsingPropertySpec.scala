package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.ConfigFactory
import org.scalacheck.Gen
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import pureconfig._
import pureconfig.generic.auto._

import PropertyTestGenerators._

/**
 * Property-based tests for config model parsing.
 *
 * These tests verify that config parsing works correctly for a wide range
 * of randomly generated inputs, catching edge cases that example-based
 * tests might miss.
 *
 * Properties tested:
 * - Roundtrip: generated HOCON parses to expected values
 * - Component order preservation
 * - Optional field handling
 * - Type safety (invalid types rejected)
 * - Edge cases (empty names, special characters)
 */
class ConfigParsingPropertySpec extends AnyFunSpec with Matchers with ScalaCheckPropertyChecks {

  // Increase the number of test cases for thorough coverage
  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  describe("PipelineConfig property tests") {

    describe("roundtrip parsing") {

      it("should parse any valid pipeline model to expected values") {
        forAll(genPipelineModel) { model =>
          val hocon  = ConfigFactory.parseString(model.toHocon)
          val result = ConfigSource.fromConfig(hocon).load[PipelineConfig]

          result.isRight shouldBe true
          val config = result.toOption.get

          config.pipelineName shouldBe model.pipelineName
          config.pipelineComponents should have size model.pipelineComponents.size
          config.failFast shouldBe model.failFast
        }
      }

      it("should preserve component order") {
        forAll(genNonEmptyPipelineModel) { model =>
          val hocon  = ConfigFactory.parseString(model.toHocon)
          val result = ConfigSource.fromConfig(hocon).load[PipelineConfig]

          result.isRight shouldBe true
          val config = result.toOption.get

          val expectedNames = model.pipelineComponents.map(_.instanceName)
          val actualNames   = config.pipelineComponents.map(_.instanceName)

          actualNames shouldBe expectedNames
        }
      }

      it("should preserve component instance types") {
        forAll(genNonEmptyPipelineModel) { model =>
          val hocon  = ConfigFactory.parseString(model.toHocon)
          val result = ConfigSource.fromConfig(hocon).load[PipelineConfig]

          result.isRight shouldBe true
          val config = result.toOption.get

          val expectedTypes = model.pipelineComponents.map(_.instanceType)
          val actualTypes   = config.pipelineComponents.map(_.instanceType)

          actualTypes shouldBe expectedTypes
        }
      }
    }

    describe("failFast handling") {

      it("should correctly parse failFast=true") {
        forAll(genPipelineModel) { model =>
          val modelWithFailFast = model.copy(failFast = true)
          val hocon             = ConfigFactory.parseString(modelWithFailFast.toHocon)
          val result            = ConfigSource.fromConfig(hocon).load[PipelineConfig]

          result.isRight shouldBe true
          result.toOption.get.failFast shouldBe true
        }
      }

      it("should correctly parse failFast=false") {
        forAll(genPipelineModel) { model =>
          val modelWithFailFast = model.copy(failFast = false)
          val hocon             = ConfigFactory.parseString(modelWithFailFast.toHocon)
          val result            = ConfigSource.fromConfig(hocon).load[PipelineConfig]

          result.isRight shouldBe true
          result.toOption.get.failFast shouldBe false
        }
      }
    }

    describe("empty components list") {

      it("should handle pipelines with no components") {
        forAll(genPipelineModel) { model =>
          val emptyModel = model.copy(pipelineComponents = Nil)
          val hocon      = ConfigFactory.parseString(emptyModel.toHocon)
          val result     = ConfigSource.fromConfig(hocon).load[PipelineConfig]

          result.isRight shouldBe true
          result.toOption.get.pipelineComponents shouldBe empty
        }
      }
    }
  }

  describe("ComponentConfig property tests") {

    describe("roundtrip parsing") {

      it("should parse any valid component model") {
        forAll(genComponentModel) { model =>
          val hocon  = ConfigFactory.parseString(model.toHocon)
          val result = ConfigSource.fromConfig(hocon).load[ComponentConfig]

          result.isRight shouldBe true
          val config = result.toOption.get

          config.instanceType shouldBe model.instanceType
          config.instanceName shouldBe model.instanceName
        }
      }

      it("should preserve instance-config keys") {
        forAll(genComponentModel) { model =>
          val hocon  = ConfigFactory.parseString(model.toHocon)
          val result = ConfigSource.fromConfig(hocon).load[ComponentConfig]

          result.isRight shouldBe true
          val config = result.toOption.get

          // Verify all keys from model are present in parsed config
          model.instanceConfig.keys.foreach(key => config.instanceConfig.hasPath(key) shouldBe true)
        }
      }
    }

    describe("empty instance-config") {

      it("should handle components with empty instance-config") {
        forAll(genComponentModel) { model =>
          val emptyConfigModel = model.copy(instanceConfig = Map.empty)
          val hocon            = ConfigFactory.parseString(emptyConfigModel.toHocon)
          val result           = ConfigSource.fromConfig(hocon).load[ComponentConfig]

          result.isRight shouldBe true
          result.toOption.get.instanceConfig.isEmpty shouldBe true
        }
      }
    }
  }

  describe("SparkConfig property tests") {

    describe("roundtrip parsing") {

      it("should parse any valid spark config model") {
        forAll(genSparkConfigModel) { model =>
          val hocon  = ConfigFactory.parseString(model.toHocon)
          val result = ConfigSource.fromConfig(hocon).load[SparkConfig]

          result.isRight shouldBe true
          val config = result.toOption.get

          config.master shouldBe model.master
          config.appName shouldBe model.appName
        }
      }

      it("should preserve spark config entries") {
        forAll(genSparkConfigModel) { model =>
          val hocon  = ConfigFactory.parseString(model.toHocon)
          val result = ConfigSource.fromConfig(hocon).load[SparkConfig]

          result.isRight shouldBe true
          val config = result.toOption.get

          model.config.foreach {
            case (key, value) =>
              config.config.get(key) shouldBe Some(value)
          }
        }
      }
    }

    describe("optional fields") {

      it("should handle missing master") {
        forAll(genSparkConfigModel) { model =>
          val noMaster = model.copy(master = None)
          val hocon    = ConfigFactory.parseString(noMaster.toHocon)
          val result   = ConfigSource.fromConfig(hocon).load[SparkConfig]

          result.isRight shouldBe true
          result.toOption.get.master shouldBe None
        }
      }

      it("should handle missing appName") {
        forAll(genSparkConfigModel) { model =>
          val noAppName = model.copy(appName = None)
          val hocon     = ConfigFactory.parseString(noAppName.toHocon)
          val result    = ConfigSource.fromConfig(hocon).load[SparkConfig]

          result.isRight shouldBe true
          result.toOption.get.appName shouldBe None
        }
      }

      it("should handle empty config map") {
        forAll(genSparkConfigModel) { model =>
          val emptyConfig = model.copy(config = Map.empty)
          val hocon       = ConfigFactory.parseString(emptyConfig.toHocon)
          val result      = ConfigSource.fromConfig(hocon).load[SparkConfig]

          result.isRight shouldBe true
          result.toOption.get.config shouldBe empty
        }
      }
    }
  }

  describe("negative property tests") {

    describe("missing required fields") {

      it("should reject PipelineConfig without pipeline-name") {
        forAll(genPipelineModel) { _ =>
          val hoconWithoutName =
            s"""{
               |  pipeline-components = []
               |}""".stripMargin

          val hocon  = ConfigFactory.parseString(hoconWithoutName)
          val result = ConfigSource.fromConfig(hocon).load[PipelineConfig]

          result.isLeft shouldBe true
        }
      }

      it("should reject PipelineConfig without pipeline-components") {
        forAll(genPipelineModel) { model =>
          val hoconWithoutComponents =
            s"""{
               |  pipeline-name = "${model.pipelineName}"
               |}""".stripMargin

          val hocon  = ConfigFactory.parseString(hoconWithoutComponents)
          val result = ConfigSource.fromConfig(hocon).load[PipelineConfig]

          result.isLeft shouldBe true
        }
      }

      it("should reject ComponentConfig without instance-type") {
        forAll(genComponentModel) { model =>
          val hoconWithoutType =
            s"""{
               |  instance-name = "${model.instanceName}"
               |  instance-config {}
               |}""".stripMargin

          val hocon  = ConfigFactory.parseString(hoconWithoutType)
          val result = ConfigSource.fromConfig(hocon).load[ComponentConfig]

          result.isLeft shouldBe true
        }
      }

      it("should reject ComponentConfig without instance-name") {
        forAll(genComponentModel) { model =>
          val hoconWithoutName =
            s"""{
               |  instance-type = "${model.instanceType}"
               |  instance-config {}
               |}""".stripMargin

          val hocon  = ConfigFactory.parseString(hoconWithoutName)
          val result = ConfigSource.fromConfig(hocon).load[ComponentConfig]

          result.isLeft shouldBe true
        }
      }

      it("should reject ComponentConfig without instance-config") {
        forAll(genComponentModel) { model =>
          val hoconWithoutConfig =
            s"""{
               |  instance-type = "${model.instanceType}"
               |  instance-name = "${model.instanceName}"
               |}""".stripMargin

          val hocon  = ConfigFactory.parseString(hoconWithoutConfig)
          val result = ConfigSource.fromConfig(hocon).load[ComponentConfig]

          result.isLeft shouldBe true
        }
      }
    }
  }

  describe("scalability property tests") {

    it("should handle pipelines with many components") {
      // Generate pipelines with 10-20 components
      val genLargePipeline = for {
        pipelineName   <- genName
        componentCount <- Gen.choose(10, 20)
        components     <- Gen.listOfN(componentCount, genComponentModel)
        failFast       <- Gen.oneOf(true, false)
      } yield PipelineModel(pipelineName, components, failFast)

      forAll(genLargePipeline) { model =>
        val hocon  = ConfigFactory.parseString(model.toHocon)
        val result = ConfigSource.fromConfig(hocon).load[PipelineConfig]

        result.isRight shouldBe true
        result.toOption.get.pipelineComponents should have size model.pipelineComponents.size
      }
    }

    it("should handle components with many config entries") {
      // Generate components with 10-20 config entries
      val genLargeConfigEntries: Gen[Map[String, ConfigValue]] = for {
        size   <- Gen.choose(10, 20)
        keys   <- Gen.listOfN(size, Gen.alphaLowerStr.suchThat(_.nonEmpty).map(_.take(15)))
        values <- Gen.listOfN(size, genConfigValue)
      } yield keys.zip(values).toMap

      val genLargeComponent = for {
        instanceType   <- genClassName
        instanceName   <- genName
        instanceConfig <- genLargeConfigEntries
      } yield ComponentModel(instanceType, instanceName, instanceConfig)

      forAll(genLargeComponent) { model =>
        val hocon  = ConfigFactory.parseString(model.toHocon)
        val result = ConfigSource.fromConfig(hocon).load[ComponentConfig]

        result.isRight shouldBe true
        model.instanceConfig.keys.foreach(key => result.toOption.get.instanceConfig.hasPath(key) shouldBe true)
      }
    }
  }
}
