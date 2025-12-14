package io.github.dwsmith1983.spark.pipeline.runtime

import io.github.dwsmith1983.spark.pipeline.config.{ConfigurableInstance, SparkConfig}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import pureconfig._
import pureconfig.generic.auto._

import scala.collection.mutable

/**
 * Comprehensive tests for DataFlow trait.
 *
 * Tests verify that DataFlow correctly provides SparkSession, logging,
 * and component execution capabilities.
 */
class DataFlowSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val config: SparkConfig = SparkConfig(
      master = Some("local[2]"),
      appName = Some("DataFlowTest"),
      config = Map(
        "spark.ui.enabled"             -> "false",
        "spark.sql.shuffle.partitions" -> "2"
      )
    )
    spark = SparkSessionWrapper.configure(config)
  }

  override def afterAll(): Unit =
    SparkSessionWrapper.stop()

  override def beforeEach(): Unit = {
    // Reset test trackers
    TestDataFlowComponent.executionLog.clear()
    FailingDataFlowComponent.executionLog.clear()
  }

  describe("DataFlow") {

    // =========================================================================
    // HAPPY PATH TESTS
    // =========================================================================

    describe("happy path") {

      it("should provide access to SparkSession") {
        val component: TestDataFlowComponent = new TestDataFlowComponent(TestDataFlowConfig("test"))

        component.spark should not be null
        component.spark shouldBe a[SparkSession]
      }

      it("should provide access to logger") {
        val component: TestDataFlowComponent = new TestDataFlowComponent(TestDataFlowConfig("test"))

        component.getLogger should not be null
      }

      it("should execute run method successfully") {
        val component: TestDataFlowComponent = new TestDataFlowComponent(TestDataFlowConfig("test-run"))

        component.run()

        TestDataFlowComponent.executionLog should contain("run:test-run")
      }

      it("should allow creating DataFrames in run") {
        val component: DataFrameCreatingComponent = new DataFrameCreatingComponent(DataFrameConfig(10))

        component.run()

        TestDataFlowComponent.executionLog should contain("created:10")
      }

      it("should allow SQL operations in run") {
        val component: SqlComponent = new SqlComponent(SqlConfig("test_table"))

        component.run()

        TestDataFlowComponent.executionLog should contain("sql:test_table")
      }

      it("should provide name from class by default") {
        val component: TestDataFlowComponent = new TestDataFlowComponent(TestDataFlowConfig("test"))

        component.name shouldBe "TestDataFlowComponent"
      }

      it("should allow overriding name") {
        val component: NamedComponent = new NamedComponent(NamedConfig("custom-name"))

        component.name shouldBe "custom-name"
      }

      it("should work with ConfigurableInstance pattern") {
        val config: Config = ConfigFactory.parseString("""
          message = "from-config"
        """)

        val component: ConfigurableDataFlowComponent = ConfigurableDataFlowComponent
          .createFromConfig(config)
          .asInstanceOf[ConfigurableDataFlowComponent]

        component.run()

        TestDataFlowComponent.executionLog should contain("configurable:from-config")
      }
    }

    // =========================================================================
    // UNHAPPY PATH TESTS
    // =========================================================================

    describe("unhappy path") {

      it("should propagate exceptions from run") {
        val component: FailingDataFlowComponent = new FailingDataFlowComponent(FailingConfig(shouldFail = true))

        val exception: RuntimeException = intercept[RuntimeException] {
          component.run()
        }

        exception.getMessage shouldBe "Intentional failure"
      }

      it("should log before failing") {
        val component: FailingDataFlowComponent = new FailingDataFlowComponent(FailingConfig(shouldFail = true))

        try {
          component.run()
        } catch {
          case _: RuntimeException => // expected
        }

        FailingDataFlowComponent.executionLog should contain("starting")
        FailingDataFlowComponent.executionLog should not contain "completed"
      }

      it("should handle spark operation failures") {
        val component: BadSparkOperationComponent = new BadSparkOperationComponent()

        // This should fail because we're trying to read a non-existent path
        val exception: Exception = intercept[Exception] {
          component.run()
        }

        exception should not be null
      }
    }

    // =========================================================================
    // EDGE CASE TESTS
    // =========================================================================

    describe("edge cases") {

      it("should handle empty run implementation") {
        val component: EmptyRunComponent = new EmptyRunComponent()

        noException should be thrownBy {
          component.run()
        }
      }

      it("should handle multiple sequential runs") {
        val component: TestDataFlowComponent = new TestDataFlowComponent(TestDataFlowConfig("multi"))

        component.run()
        component.run()
        component.run()

        TestDataFlowComponent.executionLog.count(_ == "run:multi") shouldBe 3
      }

      it("should maintain spark session across multiple components") {
        val component1: TestDataFlowComponent = new TestDataFlowComponent(TestDataFlowConfig("comp1"))
        val component2: TestDataFlowComponent = new TestDataFlowComponent(TestDataFlowConfig("comp2"))

        component1.spark shouldBe theSameInstanceAs(component2.spark)
      }

      it("should handle component creating temp views") {
        val component: TempViewComponent = new TempViewComponent(TempViewConfig("edge_test_view", 5))

        component.run()

        // Verify the view was created
        val df: DataFrame = spark.table("edge_test_view")
        df.count() shouldBe 5
      }

      it("should handle component with heavy transformations") {
        val component: HeavyTransformComponent = new HeavyTransformComponent(HeavyTransformConfig(1000))

        noException should be thrownBy {
          component.run()
        }

        TestDataFlowComponent.executionLog should contain("heavy:1000")
      }

      it("should handle unicode in logging") {
        val component: UnicodeLoggingComponent = new UnicodeLoggingComponent()

        noException should be thrownBy {
          component.run()
        }
      }
    }
  }
}

// =============================================================================
// TEST FIXTURES
// =============================================================================

// Basic test component
case class TestDataFlowConfig(id: String)

object TestDataFlowComponent {
  val executionLog: mutable.ListBuffer[String] = mutable.ListBuffer.empty
}

class TestDataFlowComponent(conf: TestDataFlowConfig) extends DataFlow {
  // Expose logger for testing
  def getLogger: org.apache.logging.log4j.Logger = logger

  override def run(): Unit = {
    logger.info(s"Running with id: ${conf.id}")
    TestDataFlowComponent.executionLog += s"run:${conf.id}"
  }
}

// DataFrame creating component
case class DataFrameConfig(rowCount: Int)

class DataFrameCreatingComponent(conf: DataFrameConfig) extends DataFlow {

  override def run(): Unit = {
    import spark.implicits._
    val df = (1 to conf.rowCount).toDF("value")
    logger.info(s"Created DataFrame with ${df.count()} rows")
    TestDataFlowComponent.executionLog += s"created:${conf.rowCount}"
  }
}

// SQL component
case class SqlConfig(tableName: String)

class SqlComponent(conf: SqlConfig) extends DataFlow {

  override def run(): Unit = {
    import spark.implicits._
    val df = Seq(1, 2, 3).toDF("id")
    df.createOrReplaceTempView(conf.tableName)
    val result = spark.sql(s"SELECT * FROM ${conf.tableName}")
    logger.info(s"SQL result count: ${result.count()}")
    TestDataFlowComponent.executionLog += s"sql:${conf.tableName}"
  }
}

// Named component
case class NamedConfig(componentName: String)

class NamedComponent(conf: NamedConfig) extends DataFlow {
  override val name: String = conf.componentName

  override def run(): Unit =
    logger.info(s"Named component: $name")
}

// Configurable component following the pattern
case class ConfigurableConfig(message: String)

object ConfigurableDataFlowComponent extends ConfigurableInstance {

  override def createFromConfig(conf: Config): ConfigurableDataFlowComponent =
    new ConfigurableDataFlowComponent(ConfigSource.fromConfig(conf).loadOrThrow[ConfigurableConfig])
}

class ConfigurableDataFlowComponent(conf: ConfigurableConfig) extends DataFlow {

  override def run(): Unit = {
    logger.info(s"Configurable: ${conf.message}")
    TestDataFlowComponent.executionLog += s"configurable:${conf.message}"
  }
}

// Failing component
case class FailingConfig(shouldFail: Boolean)

object FailingDataFlowComponent {
  val executionLog: mutable.ListBuffer[String] = mutable.ListBuffer.empty
}

class FailingDataFlowComponent(conf: FailingConfig) extends DataFlow {

  override def run(): Unit = {
    logger.info("Starting failing component")
    FailingDataFlowComponent.executionLog += "starting"

    if (conf.shouldFail) {
      throw new RuntimeException("Intentional failure")
    }

    FailingDataFlowComponent.executionLog += "completed"
  }
}

// Bad Spark operation component
class BadSparkOperationComponent extends DataFlow {

  override def run(): Unit =
    // Try to read from non-existent path
    spark.read.parquet("/non/existent/path/that/should/fail")
}

// Empty run component
class EmptyRunComponent extends DataFlow {

  override def run(): Unit = {
    // Intentionally empty
  }
}

// Temp view component
case class TempViewConfig(viewName: String, rowCount: Int)

class TempViewComponent(conf: TempViewConfig) extends DataFlow {

  override def run(): Unit = {
    import spark.implicits._
    val df = (1 to conf.rowCount).toDF("id")
    df.createOrReplaceTempView(conf.viewName)
    logger.info(s"Created temp view: ${conf.viewName}")
  }
}

// Heavy transform component
case class HeavyTransformConfig(rowCount: Int)

class HeavyTransformComponent(conf: HeavyTransformConfig) extends DataFlow {

  override def run(): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = spark.range(conf.rowCount).toDF("id")
      .withColumn("squared", col("id") * col("id"))
      .withColumn("cubed", col("id") * col("id") * col("id"))
      .groupBy(col("id") % 10)
      .agg(sum("squared"), avg("cubed"))

    df.collect() // Force execution
    logger.info(s"Heavy transform complete")
    TestDataFlowComponent.executionLog += s"heavy:${conf.rowCount}"
  }
}

// Unicode logging component
class UnicodeLoggingComponent extends DataFlow {

  override def run(): Unit = {
    logger.info("Japanese log: テスト")
    logger.info("Emoji test: rocket checkmark x")
    logger.info("Special chars: àéïõü")
  }
}
