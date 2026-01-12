package io.github.dwsmith1983.spark.pipeline.runner

import io.github.dwsmith1983.spark.pipeline.config._
import io.github.dwsmith1983.spark.pipeline.runtime.{DataFlow, SparkSessionWrapper}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import pureconfig._
import pureconfig.generic.auto._

import scala.collection.mutable
import java.nio.file.{Files, Path}

/**
 * Integration tests for SimplePipelineRunner.
 *
 * These tests verify end-to-end pipeline execution including:
 * - Config parsing
 * - SparkSession configuration
 * - Component instantiation
 * - Sequential execution
 * - Error handling
 */
class SimplePipelineRunnerSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeEach(): Unit = {
    // Stop any existing SparkSession to ensure clean state for each test
    SparkSessionWrapper.stop()
    ExecutionTracker.reset()
    HooksTracker.reset()
    HooksTracker2.reset()
  }

  override def afterAll(): Unit =
    SparkSessionWrapper.stop()

  describe("SimplePipelineRunner") {

    // =========================================================================
    // HAPPY PATH TESTS
    // =========================================================================

    describe("happy path") {

      it("should run a single component pipeline") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "SingleComponent"
          }
          pipeline {
            pipeline-name = "Single Component Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Tracker1"
                instance-config {
                  component-id = "single-1"
                }
              }
            ]
          }
        """)

        SimplePipelineRunner.run(config)

        ExecutionTracker.executedComponents should contain("single-1")
        ExecutionTracker.executedComponents should have size 1
      }

      it("should run multiple components in order") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "MultiComponent"
          }
          pipeline {
            pipeline-name = "Multi Component Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "First"
                instance-config { component-id = "first" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Second"
                instance-config { component-id = "second" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Third"
                instance-config { component-id = "third" }
              }
            ]
          }
        """)

        SimplePipelineRunner.run(config)

        ExecutionTracker.executedComponents shouldBe List("first", "second", "third")
      }

      it("should configure SparkSession from config") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[2]"
            app-name = "ConfiguredApp"
            config {
              "spark.ui.enabled" = "false"
              "spark.sql.shuffle.partitions" = "4"
            }
          }
          pipeline {
            pipeline-name = "Spark Config Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.SparkConfigVerifier"
                instance-name = "Verifier"
                instance-config {
                  expected-app-name = "ConfiguredApp"
                  expected-partitions = "4"
                }
              }
            ]
          }
        """)

        SimplePipelineRunner.run(config)

        ExecutionTracker.verifications should contain("app-name:ConfiguredApp")
        ExecutionTracker.verifications should contain("partitions:4")
      }

      it("should pass instance-config to components correctly") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "ConfigPassthrough"
          }
          pipeline {
            pipeline-name = "Config Passthrough Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.ConfigEchoComponent"
                instance-name = "Echo"
                instance-config {
                  string-value = "hello-world"
                  int-value = 42
                  nested {
                    inner-value = "nested-hello"
                  }
                  list-value = ["a", "b", "c"]
                }
              }
            ]
          }
        """)

        SimplePipelineRunner.run(config)

        ExecutionTracker.echoedConfig("string-value") shouldBe "hello-world"
        ExecutionTracker.echoedConfig("int-value") shouldBe "42"
        ExecutionTracker.echoedConfig("nested.inner-value") shouldBe "nested-hello"
        ExecutionTracker.echoedConfig("list-value") shouldBe "a,b,c"
      }

      it("should run without spark config block") {
        val config: Config = ConfigFactory.parseString("""
          pipeline {
            pipeline-name = "No Spark Config"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "NoSparkConfig"
                instance-config { component-id = "no-spark" }
              }
            ]
          }
        """)

        // Should not throw, will use default Spark config
        noException should be thrownBy {
          SimplePipelineRunner.run(config)
        }

        ExecutionTracker.executedComponents should contain("no-spark")
      }

      it("should run pipeline via main method with config file") {
        val tempFile: Path        = Files.createTempFile("pipeline-test", ".conf")
        val configContent: String = """
          spark {
            master = "local[1]"
            app-name = "MainMethodTest"
          }
          pipeline {
            pipeline-name = "Main Method Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "MainTracker"
                instance-config { component-id = "main-test" }
              }
            ]
          }
        """
        Files.writeString(tempFile, configContent)

        val originalValue: String = System.getProperty("config.file")
        try {
          System.setProperty("config.file", tempFile.toString)
          ConfigFactory.invalidateCaches()

          SimplePipelineRunner.main(Array.empty)

          ExecutionTracker.executedComponents should contain("main-test")
        } finally {
          if (originalValue != null) System.setProperty("config.file", originalValue)
          else System.clearProperty("config.file")
          ConfigFactory.invalidateCaches()
          Files.deleteIfExists(tempFile)
        }
      }
    }

    // =========================================================================
    // UNHAPPY PATH TESTS
    // =========================================================================

    describe("unhappy path") {

      it("should fail when pipeline config is missing") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          # pipeline block missing
        """)

        val exception: Exception = intercept[Exception] {
          SimplePipelineRunner.run(config)
        }

        exception.getMessage should (include("pipeline").or(include("config")))
      }

      it("should fail when component class does not exist") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Invalid Component"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.NonExistentComponent"
                instance-name = "Ghost"
                instance-config {}
              }
            ]
          }
        """)

        val exception: ComponentInstantiationException = intercept[ComponentInstantiationException] {
          SimplePipelineRunner.run(config)
        }

        exception.getMessage should include("Could not find class")
      }

      it("should fail when component throws during instantiation") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Instantiation Failure"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.FailOnInstantiateComponent"
                instance-name = "FailInstantiate"
                instance-config {}
              }
            ]
          }
        """)

        val exception: ComponentInstantiationException = intercept[ComponentInstantiationException] {
          SimplePipelineRunner.run(config)
        }

        exception.getCause shouldBe a[IllegalStateException]
      }

      it("should fail when component throws during run") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Run Failure"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.FailOnRunComponent"
                instance-name = "FailRun"
                instance-config {
                  fail-message = "Expected failure during run"
                }
              }
            ]
          }
        """)

        val exception: RuntimeException = intercept[RuntimeException] {
          SimplePipelineRunner.run(config)
        }

        exception.getMessage shouldBe "Expected failure during run"
      }

      it("should stop execution when a component fails") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Partial Execution"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "BeforeFailure"
                instance-config { component-id = "before" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.FailOnRunComponent"
                instance-name = "Failure"
                instance-config { fail-message = "stop here" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "AfterFailure"
                instance-config { component-id = "after" }
              }
            ]
          }
        """)

        try {
          SimplePipelineRunner.run(config)
        } catch {
          case _: RuntimeException => // expected
        }

        ExecutionTracker.executedComponents should contain("before")
        ExecutionTracker.executedComponents should not contain "after"
      }

      it("should fail with missing required config in component") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Missing Config"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.RequiredConfigComponent"
                instance-name = "MissingRequired"
                instance-config {
                  # required-field is missing
                }
              }
            ]
          }
        """)

        val exception: ConfigurationException = intercept[ConfigurationException] {
          SimplePipelineRunner.run(config)
        }

        exception.getMessage should include("required-field")
      }
    }

    // =========================================================================
    // EDGE CASE TESTS
    // =========================================================================

    describe("edge cases") {

      it("should handle empty pipeline-components list") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Empty Pipeline"
            pipeline-components = []
          }
        """)

        noException should be thrownBy {
          SimplePipelineRunner.run(config)
        }

        ExecutionTracker.executedComponents shouldBe empty
      }

      it("should handle component that does nothing in run") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Empty Run"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.NoOpComponent"
                instance-name = "NoOp"
                instance-config {}
              }
            ]
          }
        """)

        noException should be thrownBy {
          SimplePipelineRunner.run(config)
        }
      }

      it("should handle same component type used multiple times") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Same Component Multiple Times"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Instance1"
                instance-config { component-id = "same-type-1" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Instance2"
                instance-config { component-id = "same-type-2" }
              }
            ]
          }
        """)

        SimplePipelineRunner.run(config)

        (ExecutionTracker.executedComponents should contain).allOf("same-type-1", "same-type-2")
      }

      it("should handle component with empty instance-config") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Empty Instance Config"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.NoOpComponent"
                instance-name = "NoOpWithEmptyConfig"
                instance-config {}
              }
            ]
          }
        """)

        noException should be thrownBy {
          SimplePipelineRunner.run(config)
        }
      }

      it("should share SparkSession across all components") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "SharedSession"
          }
          pipeline {
            pipeline-name = "Shared Session Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.SessionCaptureComponent"
                instance-name = "Capture1"
                instance-config { capture-id = "session1" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.SessionCaptureComponent"
                instance-name = "Capture2"
                instance-config { capture-id = "session2" }
              }
            ]
          }
        """)

        SimplePipelineRunner.run(config)

        val session1: SparkSession = ExecutionTracker.capturedSessions("session1")
        val session2: SparkSession = ExecutionTracker.capturedSessions("session2")

        session1 shouldBe theSameInstanceAs(session2)
      }
    }

    // =========================================================================
    // PIPELINE HOOKS TESTS
    // =========================================================================

    describe("pipeline hooks") {

      it("should invoke beforePipeline and afterPipeline hooks") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "HooksTest"
          }
          pipeline {
            pipeline-name = "Hooks Test Pipeline"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "HooksTracker"
                instance-config { component-id = "hooks-test" }
              }
            ]
          }
        """)

        SimplePipelineRunner.run(config, HooksTracker)

        HooksTracker.events should contain("beforePipeline:Hooks Test Pipeline")
        (HooksTracker.events should contain).allOf(
          "beforeComponent:HooksTracker:0:1",
          "afterComponent:HooksTracker:0:1"
        )
        HooksTracker.events.last should startWith("afterPipeline:Hooks Test Pipeline:Success")
      }

      it("should invoke onComponentFailure when component fails") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Failure Hooks Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.FailOnRunComponent"
                instance-name = "FailingComponent"
                instance-config { fail-message = "intentional failure" }
              }
            ]
          }
        """)

        intercept[RuntimeException] {
          SimplePipelineRunner.run(config, HooksTracker)
        }

        HooksTracker.events should contain("onComponentFailure:FailingComponent:0")
        HooksTracker.events.exists(_.startsWith("afterPipeline:Failure Hooks Test:Failure")) shouldBe true
      }

      it("should invoke hooks in correct order for multi-component pipeline") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Multi Hook Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "First"
                instance-config { component-id = "first" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Second"
                instance-config { component-id = "second" }
              }
            ]
          }
        """)

        SimplePipelineRunner.run(config, HooksTracker)

        val events: List[String] = HooksTracker.events.toList

        // Verify order
        events.indexOf("beforePipeline:Multi Hook Test") should be < events.indexOf("beforeComponent:First:0:2")
        events.indexOf("afterComponent:First:0:2") should be < events.indexOf("beforeComponent:Second:1:2")
        events.indexOf("afterComponent:Second:1:2") should be < events.indexWhere(_.startsWith("afterPipeline"))
      }

      it("should work with PipelineHooks.compose") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Compose Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "ComposeTracker"
                instance-config { component-id = "compose-test" }
              }
            ]
          }
        """)

        val composedHooks: PipelineHooks = PipelineHooks.compose(HooksTracker, HooksTracker2)

        SimplePipelineRunner.run(config, composedHooks)

        // Both trackers should have received events
        HooksTracker.events should contain("beforePipeline:Compose Test")
        HooksTracker2.events should contain("beforePipeline:Compose Test")
      }

      it("should report correct componentsRun in success result") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Count Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "One"
                instance-config { component-id = "one" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Two"
                instance-config { component-id = "two" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Three"
                instance-config { component-id = "three" }
              }
            ]
          }
        """)

        SimplePipelineRunner.run(config, HooksTracker)

        HooksTracker.lastResult shouldBe a[PipelineResult.Success]
        HooksTracker.lastResult.asInstanceOf[PipelineResult.Success].componentsRun shouldBe 3
      }

      it("should report componentsCompleted in failure result") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Partial Failure"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "SuccessOne"
                instance-config { component-id = "success-one" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.FailOnRunComponent"
                instance-name = "Failure"
                instance-config { fail-message = "fail after one" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "NeverRun"
                instance-config { component-id = "never-run" }
              }
            ]
          }
        """)

        intercept[RuntimeException] {
          SimplePipelineRunner.run(config, HooksTracker)
        }

        HooksTracker.lastResult shouldBe a[PipelineResult.Failure]
        val failure: PipelineResult.Failure = HooksTracker.lastResult.asInstanceOf[PipelineResult.Failure]
        failure.componentsCompleted shouldBe 1
        failure.failedComponent.map(_.instanceName) shouldBe Some("Failure")
      }

      it("should invoke hooks for empty pipeline") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Empty Pipeline"
            pipeline-components = []
          }
        """)

        SimplePipelineRunner.run(config, HooksTracker)

        HooksTracker.events should contain("beforePipeline:Empty Pipeline")
        HooksTracker.events.last should startWith("afterPipeline:Empty Pipeline:Success")
        HooksTracker.lastResult shouldBe a[PipelineResult.Success]
        HooksTracker.lastResult.asInstanceOf[PipelineResult.Success].componentsRun shouldBe 0
      }

      it("should use PipelineHooks.NoOp by default") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "NoOp Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "NoOpTracker"
                instance-config { component-id = "noop-test" }
              }
            ]
          }
        """)

        // Should not throw - uses NoOp hooks by default
        noException should be thrownBy {
          SimplePipelineRunner.run(config)
        }

        ExecutionTracker.executedComponents should contain("noop-test")
      }
    }

    // =========================================================================
    // DRY-RUN MODE TESTS
    // =========================================================================

    describe("dry-run mode") {

      it("should validate config and instantiate components without running them") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "DryRun Valid"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Tracker"
                instance-config { component-id = "dry-run-test" }
              }
            ]
          }
        """)

        val result: DryRunResult = SimplePipelineRunner.dryRun(config)

        result shouldBe a[DryRunResult.Valid]
        val valid: DryRunResult.Valid = result.asInstanceOf[DryRunResult.Valid]
        valid.pipelineName shouldBe "DryRun Valid"
        valid.componentCount shouldBe 1
        valid.isValid shouldBe true
        ExecutionTracker.executedComponents shouldBe empty
      }

      it("should return Invalid with ComponentInstantiationError for nonexistent class") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Invalid Class"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.NonExistentComponent"
                instance-name = "Ghost"
                instance-config {}
              }
            ]
          }
        """)

        val result: DryRunResult = SimplePipelineRunner.dryRun(config)

        result shouldBe a[DryRunResult.Invalid]
        val invalid: DryRunResult.Invalid = result.asInstanceOf[DryRunResult.Invalid]
        invalid.errors.head shouldBe a[DryRunError.ComponentInstantiationError]
        invalid.isInvalid shouldBe true
      }

      it("should return Invalid with ConfigParseError when pipeline block is missing") {
        val config: Config = ConfigFactory.parseString("""
          spark { master = "local[1]" }
        """)

        val result: DryRunResult = SimplePipelineRunner.dryRun(config)

        result shouldBe a[DryRunResult.Invalid]
        result.asInstanceOf[DryRunResult.Invalid].errors.head shouldBe a[DryRunError.ConfigParseError]
      }

      it("should collect all component errors instead of stopping at first") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Multiple Errors"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Valid"
                instance-config { component-id = "valid" }
              },
              {
                instance-type = "io.github.dwsmith1983.NonExistent1"
                instance-name = "Ghost1"
                instance-config {}
              },
              {
                instance-type = "io.github.dwsmith1983.NonExistent2"
                instance-name = "Ghost2"
                instance-config {}
              }
            ]
          }
        """)

        val result: DryRunResult = SimplePipelineRunner.dryRun(config)

        result shouldBe a[DryRunResult.Invalid]
        result.asInstanceOf[DryRunResult.Invalid].errors should have size 2
      }
    }

    // =========================================================================
    // VALIDATE MODE TESTS
    // =========================================================================

    describe("validate mode") {

      it("should return Valid for correct configuration") {
        val config: Config = ConfigFactory.parseString("""
          pipeline {
            pipeline-name = "Valid Config"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Tracker"
                instance-config { component-id = "validate-test" }
              }
            ]
          }
        """)

        val result: ValidationResult = SimplePipelineRunner.validate(config)

        result shouldBe a[ValidationResult.Valid]
        val valid: ValidationResult.Valid = result.asInstanceOf[ValidationResult.Valid]
        valid.pipelineName shouldBe "Valid Config"
        valid.componentCount shouldBe 1
        valid.isValid shouldBe true
        // Validate does NOT execute or instantiate components
        ExecutionTracker.executedComponents shouldBe empty
      }

      it("should return Invalid for missing pipeline block") {
        val config: Config = ConfigFactory.parseString("""
          spark { master = "local[1]" }
        """)

        val result: ValidationResult = SimplePipelineRunner.validate(config)

        result shouldBe a[ValidationResult.Invalid]
        result.isInvalid shouldBe true
        val invalid: ValidationResult.Invalid = result.asInstanceOf[ValidationResult.Invalid]
        invalid.errors.head.phase shouldBe ValidationPhase.RequiredFields
      }

      it("should return Invalid for empty pipeline components list") {
        val config: Config = ConfigFactory.parseString("""
          pipeline {
            pipeline-name = "Empty Components"
            pipeline-components = []
          }
        """)

        val result: ValidationResult = SimplePipelineRunner.validate(config)

        result shouldBe a[ValidationResult.Invalid]
        val invalid: ValidationResult.Invalid = result.asInstanceOf[ValidationResult.Invalid]
        invalid.errors.exists(_.message.contains("at least one component")) shouldBe true
      }

      it("should return Invalid for nonexistent component class") {
        val config: Config = ConfigFactory.parseString("""
          pipeline {
            pipeline-name = "Invalid Class"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.NonExistentComponent"
                instance-name = "Ghost"
                instance-config {}
              }
            ]
          }
        """)

        val result: ValidationResult = SimplePipelineRunner.validate(config)

        result shouldBe a[ValidationResult.Invalid]
        val invalid: ValidationResult.Invalid = result.asInstanceOf[ValidationResult.Invalid]
        invalid.errors.head.phase shouldBe ValidationPhase.TypeResolution
        invalid.errors.head.message should include("Class not found")
      }

      it("should return Invalid when companion object does not exist") {
        val config: Config = ConfigFactory.parseString("""
          pipeline {
            pipeline-name = "No Companion"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.NoCompanionComponent"
                instance-name = "NoCompanion"
                instance-config {}
              }
            ]
          }
        """)

        val result: ValidationResult = SimplePipelineRunner.validate(config)

        result shouldBe a[ValidationResult.Invalid]
        val invalid: ValidationResult.Invalid = result.asInstanceOf[ValidationResult.Invalid]
        invalid.errors.head.phase shouldBe ValidationPhase.TypeResolution
        invalid.errors.head.message should include("Companion object not found")
      }

      it("should return Invalid when companion does not extend ConfigurableInstance") {
        val config: Config = ConfigFactory.parseString("""
          pipeline {
            pipeline-name = "Bad Companion"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.BadCompanionComponent"
                instance-name = "BadCompanion"
                instance-config {}
              }
            ]
          }
        """)

        val result: ValidationResult = SimplePipelineRunner.validate(config)

        result shouldBe a[ValidationResult.Invalid]
        val invalid: ValidationResult.Invalid = result.asInstanceOf[ValidationResult.Invalid]
        invalid.errors.head.phase shouldBe ValidationPhase.ComponentConfig
        invalid.errors.head.message should include("does not extend ConfigurableInstance")
      }

      it("should collect all errors instead of stopping at first") {
        val config: Config = ConfigFactory.parseString("""
          pipeline {
            pipeline-name = "Multiple Errors"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Valid"
                instance-config { component-id = "valid" }
              },
              {
                instance-type = "io.github.dwsmith1983.NonExistent1"
                instance-name = "Ghost1"
                instance-config {}
              },
              {
                instance-type = "io.github.dwsmith1983.NonExistent2"
                instance-name = "Ghost2"
                instance-config {}
              }
            ]
          }
        """)

        val result: ValidationResult = SimplePipelineRunner.validate(config)

        result shouldBe a[ValidationResult.Invalid]
        result.asInstanceOf[ValidationResult.Invalid].errors should have size 2
      }

      it("should include location information in errors") {
        val config: Config = ConfigFactory.parseString("""
          pipeline {
            pipeline-name = "Error Location"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.NonExistent"
                instance-name = "GhostComponent"
                instance-config {}
              }
            ]
          }
        """)

        val result: ValidationResult = SimplePipelineRunner.validate(config)

        result shouldBe a[ValidationResult.Invalid]
        val error: ValidationError = result.asInstanceOf[ValidationResult.Invalid].errors.head
        error.location shouldBe a[ErrorLocation.Component]
        val componentLoc: ErrorLocation.Component = error.location.asInstanceOf[ErrorLocation.Component]
        componentLoc.index shouldBe 0
        componentLoc.instanceName shouldBe "GhostComponent"
      }

      it("should support ValidationResult combine operator") {
        val valid1: ValidationResult = ValidationResult.Valid("Test", 1)
        val valid2: ValidationResult =
          ValidationResult.Valid("Test", 1, List(ValidationWarning(ErrorLocation.Pipeline, "warn")))
        val invalid: ValidationResult = ValidationResult.Invalid(
          List(ValidationError(ValidationPhase.TypeResolution, ErrorLocation.Pipeline, "error"))
        )

        // Valid ++ Valid = Valid with combined warnings
        val combined1: ValidationResult = valid1 ++ valid2
        combined1 shouldBe a[ValidationResult.Valid]
        combined1.warnings should have size 1

        // Valid ++ Invalid = Invalid
        val combined2: ValidationResult = valid1 ++ invalid
        combined2 shouldBe a[ValidationResult.Invalid]

        // Invalid ++ Invalid = Invalid with combined errors
        val combined3: ValidationResult = invalid ++ invalid
        combined3 shouldBe a[ValidationResult.Invalid]
        combined3.errors should have size 2
      }

      it("should validate from HOCON string") {
        val hocon: String = """
          pipeline {
            pipeline-name = "From String"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Tracker"
                instance-config { component-id = "string-test" }
              }
            ]
          }
        """

        val result: ValidationResult = ConfigValidator.validateFromString(hocon)

        result shouldBe a[ValidationResult.Valid]
      }

      it("should return ConfigSyntax error for malformed HOCON") {
        val malformedHocon: String = """
          pipeline {
            pipeline-name = "Malformed
            # Missing closing quote
          }
        """

        val result: ValidationResult = ConfigValidator.validateFromString(malformedHocon)

        result shouldBe a[ValidationResult.Invalid]
        result.asInstanceOf[ValidationResult.Invalid].errors.head.phase shouldBe ValidationPhase.ConfigSyntax
      }

      it("should be faster than dryRun (does not instantiate)") {
        // This is a behavioral test - validate should NOT call createFromConfig
        val config: Config = ConfigFactory.parseString("""
          pipeline {
            pipeline-name = "No Instantiation"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Tracker"
                instance-config { component-id = "no-instantiate" }
              }
            ]
          }
        """)

        SimplePipelineRunner.validate(config)

        // The component should NOT be instantiated (which would trigger tracking)
        ExecutionTracker.executedComponents shouldBe empty
      }

      it("should validate multiple components successfully") {
        val config: Config = ConfigFactory.parseString("""
          pipeline {
            pipeline-name = "Multi Component Validate"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "First"
                instance-config { component-id = "first" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.NoOpComponent"
                instance-name = "Second"
                instance-config {}
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Third"
                instance-config { component-id = "third" }
              }
            ]
          }
        """)

        val result: ValidationResult = SimplePipelineRunner.validate(config)

        result shouldBe a[ValidationResult.Valid]
        result.asInstanceOf[ValidationResult.Valid].componentCount shouldBe 3
      }
    }

    // =========================================================================
    // FAIL-FAST / CONTINUE-ON-ERROR TESTS
    // =========================================================================

    describe("failFast mode") {

      it("should continue execution and return PartialSuccess when failFast is false") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Continue On Error"
            fail-fast = false
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "First"
                instance-config { component-id = "continue-first" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.FailOnRunComponent"
                instance-name = "Failure"
                instance-config { fail-message = "expected failure" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Third"
                instance-config { component-id = "continue-third" }
              }
            ]
          }
        """)

        noException should be thrownBy {
          SimplePipelineRunner.run(config, HooksTracker)
        }

        (ExecutionTracker.executedComponents should contain).allOf("continue-first", "continue-third")
        HooksTracker.lastResult shouldBe a[PipelineResult.PartialSuccess]
        val partial: PipelineResult.PartialSuccess = HooksTracker.lastResult.asInstanceOf[PipelineResult.PartialSuccess]
        partial.componentsSucceeded shouldBe 2
        partial.componentsFailed shouldBe 1
        partial.failures.head._1.instanceName shouldBe "Failure"
      }

      it("should return Success when failFast is false and all components succeed") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "All Success"
            fail-fast = false
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "One"
                instance-config { component-id = "success-1" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Two"
                instance-config { component-id = "success-2" }
              }
            ]
          }
        """)

        SimplePipelineRunner.run(config, HooksTracker)

        HooksTracker.lastResult shouldBe a[PipelineResult.Success]
      }

      it("should stop and throw on first failure when failFast is true (default)") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "FailFast Default"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "Before"
                instance-config { component-id = "failfast-before" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.FailOnRunComponent"
                instance-name = "Failure"
                instance-config { fail-message = "stop here" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.TrackingComponent"
                instance-name = "After"
                instance-config { component-id = "failfast-after" }
              }
            ]
          }
        """)

        intercept[RuntimeException] {
          SimplePipelineRunner.run(config)
        }

        ExecutionTracker.executedComponents should contain("failfast-before")
        ExecutionTracker.executedComponents should not contain "failfast-after"
      }

      it("should invoke onComponentFailure for each failure when failFast is false") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
          }
          pipeline {
            pipeline-name = "Multiple Failures"
            fail-fast = false
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.FailOnRunComponent"
                instance-name = "Fail1"
                instance-config { fail-message = "one" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.FailOnRunComponent"
                instance-name = "Fail2"
                instance-config { fail-message = "two" }
              }
            ]
          }
        """)

        SimplePipelineRunner.run(config, HooksTracker)

        HooksTracker.events should contain("onComponentFailure:Fail1:0")
        HooksTracker.events should contain("onComponentFailure:Fail2:1")
      }
    }
  }
}

// =============================================================================
// HOOKS TRACKER - Tracks hook invocations for testing
// =============================================================================

object HooksTracker extends PipelineHooks {
  val events: mutable.ListBuffer[String] = mutable.ListBuffer.empty
  var lastResult: PipelineResult         = _

  def reset(): Unit = {
    events.clear()
    lastResult = null
  }

  override def beforePipeline(config: PipelineConfig): Unit =
    events += s"beforePipeline:${config.pipelineName}"

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    val resultType: String = result match {
      case _: PipelineResult.Success        => "Success"
      case _: PipelineResult.Failure        => "Failure"
      case _: PipelineResult.PartialSuccess => "PartialSuccess"
    }
    events += s"afterPipeline:${config.pipelineName}:$resultType"
    lastResult = result
  }

  override def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit =
    events += s"beforeComponent:${config.instanceName}:$index:$total"

  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    val _ = durationMs // suppress unused warning
    events += s"afterComponent:${config.instanceName}:$index:$total"
  }

  override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {
    val _ = error // suppress unused warning
    events += s"onComponentFailure:${config.instanceName}:$index"
  }
}

object HooksTracker2 extends PipelineHooks {
  val events: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  def reset(): Unit = events.clear()

  override def beforePipeline(config: PipelineConfig): Unit =
    events += s"beforePipeline:${config.pipelineName}"

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit =
    events += s"afterPipeline:${config.pipelineName}"
}

// =============================================================================
// EXECUTION TRACKER - Shared state for test verification
// =============================================================================

object ExecutionTracker {
  val executedComponents: mutable.ListBuffer[String]      = mutable.ListBuffer.empty
  val verifications: mutable.ListBuffer[String]           = mutable.ListBuffer.empty
  val echoedConfig: mutable.Map[String, String]           = mutable.Map.empty
  val capturedSessions: mutable.Map[String, SparkSession] = mutable.Map.empty

  def reset(): Unit = {
    executedComponents.clear()
    verifications.clear()
    echoedConfig.clear()
    capturedSessions.clear()
  }
}

// =============================================================================
// TEST COMPONENTS
// =============================================================================

// Simple tracking component
case class TrackingConfig(componentId: String)

object TrackingComponent extends ConfigurableInstance {

  override def createFromConfig(conf: Config): TrackingComponent =
    new TrackingComponent(ConfigSource.fromConfig(conf).loadOrThrow[TrackingConfig])
}

class TrackingComponent(conf: TrackingConfig) extends DataFlow {

  override def run(): Unit = {
    logger.info(s"Tracking component executed: ${conf.componentId}")
    ExecutionTracker.executedComponents += conf.componentId
  }
}

// Spark config verifier
case class SparkConfigVerifierConfig(expectedAppName: String, expectedPartitions: String)

object SparkConfigVerifier extends ConfigurableInstance {

  override def createFromConfig(conf: Config): SparkConfigVerifier =
    new SparkConfigVerifier(ConfigSource.fromConfig(conf).loadOrThrow[SparkConfigVerifierConfig])
}

class SparkConfigVerifier(conf: SparkConfigVerifierConfig) extends DataFlow {

  override def run(): Unit = {
    ExecutionTracker.verifications += s"app-name:${spark.sparkContext.appName}"
    ExecutionTracker.verifications += s"partitions:${spark.conf.get("spark.sql.shuffle.partitions")}"
  }
}

// Config echo component
case class ConfigEchoConfig(
  stringValue: String,
  intValue: Int,
  nested: NestedEchoConfig,
  listValue: List[String])
case class NestedEchoConfig(innerValue: String)

object ConfigEchoComponent extends ConfigurableInstance {

  override def createFromConfig(conf: Config): ConfigEchoComponent =
    new ConfigEchoComponent(ConfigSource.fromConfig(conf).loadOrThrow[ConfigEchoConfig])
}

class ConfigEchoComponent(conf: ConfigEchoConfig) extends DataFlow {

  override def run(): Unit = {
    ExecutionTracker.echoedConfig("string-value") = conf.stringValue
    ExecutionTracker.echoedConfig("int-value") = conf.intValue.toString
    ExecutionTracker.echoedConfig("nested.inner-value") = conf.nested.innerValue
    ExecutionTracker.echoedConfig("list-value") = conf.listValue.mkString(",")
  }
}

// Fail on instantiate component
object FailOnInstantiateComponent extends ConfigurableInstance {

  override def createFromConfig(conf: Config): Any =
    throw new IllegalStateException("Intentional instantiation failure")
}

// Fail on run component
case class FailOnRunConfig(failMessage: String)

object FailOnRunComponent extends ConfigurableInstance {

  override def createFromConfig(conf: Config): FailOnRunComponent =
    new FailOnRunComponent(ConfigSource.fromConfig(conf).loadOrThrow[FailOnRunConfig])
}

class FailOnRunComponent(conf: FailOnRunConfig) extends DataFlow {

  override def run(): Unit =
    throw new RuntimeException(conf.failMessage)
}

// Required config component
case class RequiredOnlyConfig(requiredField: String)

object RequiredConfigComponent extends ConfigurableInstance {

  override def createFromConfig(conf: Config): RequiredConfigComponent =
    new RequiredConfigComponent(ConfigSource.fromConfig(conf).loadOrThrow[RequiredOnlyConfig])
}

class RequiredConfigComponent(conf: RequiredOnlyConfig) extends DataFlow {

  override def run(): Unit =
    logger.info(s"Required: ${conf.requiredField}")
}

// No-op component - does nothing, accepts empty config
object NoOpComponent extends ConfigurableInstance {

  override def createFromConfig(conf: Config): NoOpComponent =
    new NoOpComponent()
}

class NoOpComponent extends DataFlow {

  override def run(): Unit = ()
}

// Session capture component
case class SessionCaptureConfig(captureId: String)

object SessionCaptureComponent extends ConfigurableInstance {

  override def createFromConfig(conf: Config): SessionCaptureComponent =
    new SessionCaptureComponent(ConfigSource.fromConfig(conf).loadOrThrow[SessionCaptureConfig])
}

class SessionCaptureComponent(conf: SessionCaptureConfig) extends DataFlow {

  override def run(): Unit =
    ExecutionTracker.capturedSessions(conf.captureId) = spark
}

// Component without companion object (for validation testing)
class NoCompanionComponent extends DataFlow {

  override def run(): Unit = ()
}

// Component with companion that does NOT extend ConfigurableInstance
object BadCompanionComponent {
  // Does not extend ConfigurableInstance
  def create(): BadCompanionComponent = new BadCompanionComponent()
}

class BadCompanionComponent extends DataFlow {

  override def run(): Unit = ()
}
