package io.github.dwsmith1983.spark.pipeline.runtime

import io.github.dwsmith1983.spark.pipeline.config.SparkConfig
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll

/**
 * Tests for SparkConnector and its implementations.
 *
 * Note: These tests focus on the connector selection logic and LocalSparkConnector.
 * SparkConnectConnector requires a running Spark Connect server and is tested separately.
 */
class SparkConnectorSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeEach(): Unit =
    // Stop any existing SparkSession to ensure clean state
    SparkSessionWrapper.stop()

  override def afterAll(): Unit =
    SparkSessionWrapper.stop()

  describe("SparkConnector factory") {

    describe("connector selection") {

      it("should select LocalSparkConnector when connectString is not provided") {
        val config: SparkConfig       = SparkConfig(master = Some("local[1]"), appName = Some("LocalTest"))
        val connector: SparkConnector = SparkConnector(config)

        connector shouldBe a[LocalSparkConnector]
      }

      it("should select SparkConnectConnector when connectString is provided") {
        val config: SparkConfig = SparkConfig(
          connectString = Some("sc://localhost:15002"),
          appName = Some("ConnectTest")
        )
        val connector: SparkConnector = SparkConnector(config)

        connector shouldBe a[SparkConnectConnector]
      }

      it("should prefer connectString over master when both are provided") {
        val config: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          connectString = Some("sc://localhost:15002"),
          appName = Some("BothTest")
        )
        val connector: SparkConnector = SparkConnector(config)

        connector shouldBe a[SparkConnectConnector]
      }
    }
  }

  describe("LocalSparkConnector") {

    describe("happy path") {

      it("should create a SparkSession with master configuration") {
        val config: SparkConfig            = SparkConfig(master = Some("local[1]"), appName = Some("LocalTest"))
        val connector: LocalSparkConnector = new LocalSparkConnector(config)

        val session: SparkSession = connector.getSession

        session should not be null
        session.sparkContext.master shouldBe "local[1]"
        session.sparkContext.appName shouldBe "LocalTest"

        session.stop()
      }

      it("should apply additional Spark configuration") {
        val config: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("ConfigTest"),
          config = Map(
            "spark.ui.enabled"             -> "false",
            "spark.sql.shuffle.partitions" -> "2"
          )
        )
        val connector: LocalSparkConnector = new LocalSparkConnector(config)

        val session: SparkSession = connector.getSession

        session.conf.get("spark.sql.shuffle.partitions") shouldBe "2"

        session.stop()
      }

      it("should return existing session when called multiple times") {
        val config: SparkConfig            = SparkConfig(master = Some("local[1]"), appName = Some("ReuseTest"))
        val connector: LocalSparkConnector = new LocalSparkConnector(config)

        val session1: SparkSession = connector.getSession
        val session2: SparkSession = connector.getSession

        session1 shouldBe theSameInstanceAs(session2)

        session1.stop()
      }
    }

    describe("edge cases") {

      it("should handle empty config map") {
        val config: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("EmptyConfig"),
          config = Map.empty
        )
        val connector: LocalSparkConnector = new LocalSparkConnector(config)

        val session: SparkSession = connector.getSession

        session should not be null

        session.stop()
      }

      it("should handle missing master gracefully when no existing session") {
        val config: SparkConfig = SparkConfig(
          master = None,
          appName = Some("NoMaster")
        )
        val connector: LocalSparkConnector = new LocalSparkConnector(config)

        // Should throw when trying to create session without master
        an[Exception] should be thrownBy {
          connector.getSession
        }
      }
    }
  }

  describe("SparkConnectConnector") {

    describe("validation") {

      it("should require connectString to be defined") {
        val config: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("NoConnectString")
        )
        val connector: SparkConnectConnector = new SparkConnectConnector(config)

        val exception: IllegalArgumentException = the[IllegalArgumentException] thrownBy {
          connector.getSession
        }

        exception.getMessage should include("connectString must be defined")
      }
    }

    describe("Spark version compatibility") {

      it("should provide clear error when Spark Connect is not available") {
        val config: SparkConfig = SparkConfig(
          connectString = Some("sc://localhost:15002"),
          appName = Some("ConnectTest")
        )
        val connector: SparkConnectConnector = new SparkConnectConnector(config)

        // This will fail on Spark versions < 3.4 with UnsupportedOperationException
        // On Spark 3.4+ it will fail because there's no server running
        val exception: Exception = the[Exception] thrownBy {
          connector.getSession
        }

        // Either:
        // - UnsupportedOperationException on Spark < 3.4
        // - Some other exception on Spark 3.4+ (server not running)
        exception shouldBe an[Exception]
      }
    }

    describe("configuration") {

      it("should construct connector with Databricks configuration") {
        val config: SparkConfig = SparkConfig(
          connectString = Some("sc://workspace.cloud.databricks.com"),
          appName = Some("DatabricksTest"),
          config = Map(
            "spark.databricks.token" -> "dapi123456"
          )
        )

        // Just verify we can create the connector
        val connector: SparkConnectConnector = new SparkConnectConnector(config)

        connector should not be null
        // We can't test getSession without a real Databricks environment
      }

      it("should handle additional Spark config with connect string") {
        val config: SparkConfig = SparkConfig(
          connectString = Some("sc://localhost:15002"),
          appName = Some("ConfigTest"),
          config = Map(
            "spark.executor.memory" -> "4g"
          )
        )

        val connector: SparkConnectConnector = new SparkConnectConnector(config)

        connector should not be null
        // We can't test getSession without a running server
      }
    }
  }
}
