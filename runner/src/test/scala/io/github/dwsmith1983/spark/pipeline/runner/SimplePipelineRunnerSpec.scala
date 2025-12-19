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

        val exception: ComponentInstantiationException = intercept[ComponentInstantiationException] {
          SimplePipelineRunner.run(config)
        }

        exception.getMessage should include("Failed to instantiate")
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
  }
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
