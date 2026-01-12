package io.github.dwsmith1983.spark.pipeline.runner

import io.github.dwsmith1983.spark.pipeline.config._
import io.github.dwsmith1983.spark.pipeline.runtime.{DataFlow, SparkSessionWrapper}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import pureconfig._
import pureconfig.generic.auto._

import scala.collection.mutable

/**
 * Integration tests for schema contract validation in SimplePipelineRunner.
 */
class SchemaValidationIntegrationSpec
  extends AnyFunSpec
  with Matchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  override def beforeEach(): Unit = {
    SparkSessionWrapper.stop()
    SchemaTestTracker.reset()
  }

  override def afterAll(): Unit =
    SparkSessionWrapper.stop()

  describe("SimplePipelineRunner schema validation") {

    describe("disabled by default") {

      it("should run without validation when schema-validation not configured") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "NoValidation"
            config { "spark.ui.enabled" = "false" }
          }
          pipeline {
            pipeline-name = "No Validation Test"
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.SchemaProducerComponent"
                instance-name = "Producer"
                instance-config { id = "producer-1" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.SchemaConsumerComponent"
                instance-name = "Consumer"
                instance-config { id = "consumer-1" }
              }
            ]
          }
        """)

        noException should be thrownBy {
          SimplePipelineRunner.run(config)
        }

        SchemaTestTracker.executed should contain allOf ("producer-1", "consumer-1")
      }
    }

    describe("with validation enabled") {

      it("should pass validation when schemas are compatible") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "CompatibleSchemas"
            config { "spark.ui.enabled" = "false" }
          }
          pipeline {
            pipeline-name = "Compatible Schema Test"
            schema-validation {
              enabled = true
            }
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.SchemaProducerComponent"
                instance-name = "Producer"
                instance-config { id = "producer-2" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.CompatibleConsumerComponent"
                instance-name = "Consumer"
                instance-config { id = "consumer-2" }
              }
            ]
          }
        """)

        noException should be thrownBy {
          SimplePipelineRunner.run(config)
        }

        SchemaTestTracker.executed should contain allOf ("producer-2", "consumer-2")
      }

      it("should fail validation when schemas are incompatible") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "IncompatibleSchemas"
            config { "spark.ui.enabled" = "false" }
          }
          pipeline {
            pipeline-name = "Incompatible Schema Test"
            schema-validation {
              enabled = true
            }
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.SchemaProducerComponent"
                instance-name = "Producer"
                instance-config { id = "producer-3" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.IncompatibleConsumerComponent"
                instance-name = "Consumer"
                instance-config { id = "consumer-3" }
              }
            ]
          }
        """)

        val exception = intercept[SchemaContractViolationException] {
          SimplePipelineRunner.run(config)
        }

        exception.producerName shouldBe "Producer"
        exception.consumerName shouldBe "Consumer"
        exception.errors should not be empty

        // Producer should have run, but consumer should not
        SchemaTestTracker.executed should contain("producer-3")
        SchemaTestTracker.executed should not contain "consumer-3"
      }

      it("should warn when only one component has schema") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "PartialSchema"
            config { "spark.ui.enabled" = "false" }
          }
          pipeline {
            pipeline-name = "Partial Schema Test"
            schema-validation {
              enabled = true
            }
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.SchemaProducerComponent"
                instance-name = "Producer"
                instance-config { id = "producer-4" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.NoSchemaComponent"
                instance-name = "NoSchema"
                instance-config { id = "no-schema-4" }
              }
            ]
          }
        """)

        // Should not fail, just warn
        noException should be thrownBy {
          SimplePipelineRunner.run(config)
        }

        SchemaTestTracker.executed should contain allOf ("producer-4", "no-schema-4")
      }
    }

    describe("strict mode") {

      it("should fail in strict mode when component lacks schema") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "StrictMode"
            config { "spark.ui.enabled" = "false" }
          }
          pipeline {
            pipeline-name = "Strict Mode Test"
            schema-validation {
              enabled = true
              strict = true
            }
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.SchemaProducerComponent"
                instance-name = "Producer"
                instance-config { id = "producer-5" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.NoSchemaComponent"
                instance-name = "NoSchema"
                instance-config { id = "no-schema-5" }
              }
            ]
          }
        """)

        val exception = intercept[SchemaContractViolationException] {
          SimplePipelineRunner.run(config)
        }

        exception.errors should not be empty
      }
    }

    describe("fail-on-warning mode") {

      it("should fail when warnings occur with fail-on-warning enabled") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "FailOnWarning"
            config { "spark.ui.enabled" = "false" }
          }
          pipeline {
            pipeline-name = "Fail On Warning Test"
            schema-validation {
              enabled = true
              fail-on-warning = true
            }
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.SchemaProducerComponent"
                instance-name = "Producer"
                instance-config { id = "producer-6" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.NoSchemaComponent"
                instance-name = "NoSchema"
                instance-config { id = "no-schema-6" }
              }
            ]
          }
        """)

        val exception = intercept[SchemaContractViolationException] {
          SimplePipelineRunner.run(config)
        }

        exception.warnings should not be empty
      }
    }

    describe("continue-on-error mode with schema validation") {

      it("should continue after schema violation with failFast=false") {
        val config: Config = ConfigFactory.parseString("""
          spark {
            master = "local[1]"
            app-name = "ContinueOnError"
            config { "spark.ui.enabled" = "false" }
          }
          pipeline {
            pipeline-name = "Continue On Error Test"
            fail-fast = false
            schema-validation {
              enabled = true
            }
            pipeline-components = [
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.SchemaProducerComponent"
                instance-name = "Producer"
                instance-config { id = "producer-7" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.IncompatibleConsumerComponent"
                instance-name = "BadConsumer"
                instance-config { id = "bad-consumer-7" }
              },
              {
                instance-type = "io.github.dwsmith1983.spark.pipeline.runner.NoSchemaComponent"
                instance-name = "ThirdComponent"
                instance-config { id = "third-7" }
              }
            ]
          }
        """)

        // Should not throw, but should record failure
        noException should be thrownBy {
          SimplePipelineRunner.run(config)
        }

        // First component ran
        SchemaTestTracker.executed should contain("producer-7")
        // Second component was skipped due to schema violation
        SchemaTestTracker.executed should not contain "bad-consumer-7"
        // Third component ran (after failure was recorded)
        SchemaTestTracker.executed should contain("third-7")
      }
    }
  }
}

// =============================================================================
// TEST FIXTURES FOR SCHEMA VALIDATION
// =============================================================================

/** Tracks execution for schema validation tests. */
object SchemaTestTracker {
  val executed: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  def reset(): Unit = executed.clear()
}

// Producer with output schema
case class SchemaProducerConfig(id: String)

object SchemaProducerComponent extends ConfigurableInstance {
  override def createFromConfig(conf: Config): SchemaProducerComponent =
    new SchemaProducerComponent(ConfigSource.fromConfig(conf).loadOrThrow[SchemaProducerConfig])
}

class SchemaProducerComponent(conf: SchemaProducerConfig) extends DataFlow with SchemaContract {
  override def outputContract: Option[SchemaDefinition] = Some(
    SchemaDefinition(Seq(
      SchemaField("id", "long", nullable = false),
      SchemaField("name", "string"),
      SchemaField("value", "double")
    ))
  )

  override def run(): Unit = {
    logger.info(s"SchemaProducerComponent running: ${conf.id}")
    SchemaTestTracker.executed += conf.id
  }
}

// Consumer with input schema (no output schema for testing)
case class SchemaConsumerConfig(id: String)

object SchemaConsumerComponent extends ConfigurableInstance {
  override def createFromConfig(conf: Config): SchemaConsumerComponent =
    new SchemaConsumerComponent(ConfigSource.fromConfig(conf).loadOrThrow[SchemaConsumerConfig])
}

class SchemaConsumerComponent(conf: SchemaConsumerConfig) extends DataFlow with SchemaContract {
  override def inputContract: Option[SchemaDefinition] = Some(
    SchemaDefinition(Seq(
      SchemaField("id", "long"),
      SchemaField("name", "string")
    ))
  )

  override def run(): Unit = {
    logger.info(s"SchemaConsumerComponent running: ${conf.id}")
    SchemaTestTracker.executed += conf.id
  }
}

// Compatible consumer (subset of producer output)
case class CompatibleConsumerConfig(id: String)

object CompatibleConsumerComponent extends ConfigurableInstance {
  override def createFromConfig(conf: Config): CompatibleConsumerComponent =
    new CompatibleConsumerComponent(ConfigSource.fromConfig(conf).loadOrThrow[CompatibleConsumerConfig])
}

class CompatibleConsumerComponent(conf: CompatibleConsumerConfig) extends DataFlow with SchemaContract {
  override def inputContract: Option[SchemaDefinition] = Some(
    SchemaDefinition(Seq(
      SchemaField("id", "long"),
      SchemaField("name", "string")
    ))
  )

  override def run(): Unit = {
    logger.info(s"CompatibleConsumerComponent running: ${conf.id}")
    SchemaTestTracker.executed += conf.id
  }
}

// Incompatible consumer (requires field not in producer output)
case class IncompatibleConsumerConfig(id: String)

object IncompatibleConsumerComponent extends ConfigurableInstance {
  override def createFromConfig(conf: Config): IncompatibleConsumerComponent =
    new IncompatibleConsumerComponent(ConfigSource.fromConfig(conf).loadOrThrow[IncompatibleConsumerConfig])
}

class IncompatibleConsumerComponent(conf: IncompatibleConsumerConfig) extends DataFlow with SchemaContract {
  override def inputContract: Option[SchemaDefinition] = Some(
    SchemaDefinition(Seq(
      SchemaField("id", "long"),
      SchemaField("name", "string"),
      SchemaField("missing_field", "integer")  // This field is not in producer output
    ))
  )

  override def run(): Unit = {
    logger.info(s"IncompatibleConsumerComponent running: ${conf.id}")
    SchemaTestTracker.executed += conf.id
  }
}

// Component without schema contract
case class NoSchemaConfig(id: String)

object NoSchemaComponent extends ConfigurableInstance {
  override def createFromConfig(conf: Config): NoSchemaComponent =
    new NoSchemaComponent(ConfigSource.fromConfig(conf).loadOrThrow[NoSchemaConfig])
}

class NoSchemaComponent(conf: NoSchemaConfig) extends DataFlow {
  override def run(): Unit = {
    logger.info(s"NoSchemaComponent running: ${conf.id}")
    SchemaTestTracker.executed += conf.id
  }
}
