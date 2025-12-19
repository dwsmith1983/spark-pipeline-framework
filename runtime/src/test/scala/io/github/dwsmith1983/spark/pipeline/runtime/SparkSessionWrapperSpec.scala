package io.github.dwsmith1983.spark.pipeline.runtime

import io.github.dwsmith1983.spark.pipeline.config.SparkConfig
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll

/**
 * Comprehensive tests for SparkSessionWrapper.
 *
 * Note: These tests use local[*] Spark mode and manage session lifecycle carefully
 * to avoid conflicts between tests.
 */
class SparkSessionWrapperSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  override def beforeEach(): Unit =
    // Stop any existing SparkSession to ensure clean state for each test
    SparkSessionWrapper.stop()

  override def afterAll(): Unit =
    // Clean up any lingering sessions
    SparkSessionWrapper.stop()

  describe("SparkSessionWrapper") {

    // =========================================================================
    // HAPPY PATH TESTS
    // =========================================================================

    describe("happy path") {

      it("should create a SparkSession when configure is called") {
        val config: SparkConfig   = SparkConfig(master = Some("local[1]"), appName = Some("CreateTest"))
        val session: SparkSession = SparkSessionWrapper.configure(config)

        session should not be null
        session.sparkContext.isStopped shouldBe false
      }

      it("should return the same session on subsequent getOrCreate calls") {
        val config: SparkConfig = SparkConfig(master = Some("local[1]"), appName = Some("SameSessionTest"))
        SparkSessionWrapper.configure(config)

        val session1: SparkSession = SparkSessionWrapper.getOrCreate()
        val session2: SparkSession = SparkSessionWrapper.getOrCreate()

        session1 shouldBe theSameInstanceAs(session2)
      }

      it("should configure session with provided SparkConfig") {
        val config: SparkConfig = SparkConfig(
          master = Some("local[2]"),
          appName = Some("TestApp"),
          config = Map(
            "spark.ui.enabled"             -> "false",
            "spark.sql.shuffle.partitions" -> "4"
          )
        )

        val session: SparkSession = SparkSessionWrapper.configure(config)

        session.sparkContext.master shouldBe "local[2]"
        session.sparkContext.appName shouldBe "TestApp"
        session.conf.get("spark.sql.shuffle.partitions") shouldBe "4"
      }

      it("should return configured session from getOrCreate after configure") {
        val config: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("ConfiguredFirst")
        )

        val configuredSession: SparkSession = SparkSessionWrapper.configure(config)
        val retrievedSession: SparkSession  = SparkSessionWrapper.getOrCreate()

        retrievedSession shouldBe theSameInstanceAs(configuredSession)
      }

      it("should provide spark session through trait mixin") {
        val config: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("TraitTest")
        )
        SparkSessionWrapper.configure(config)

        val component: TestSparkComponent = new TestSparkComponent()

        component.spark should not be null
        component.spark.sparkContext.appName shouldBe "TraitTest"
      }

      it("should provide sparkContext through trait mixin") {
        SparkSessionWrapper.configure(SparkConfig(master = Some("local[1]")))

        val component: TestSparkComponent = new TestSparkComponent()

        component.sparkContext should not be null
        component.sparkContext shouldBe component.spark.sparkContext
      }

      it("should provide sqlContext through trait mixin") {
        SparkSessionWrapper.configure(SparkConfig(master = Some("local[1]")))

        val component: TestSparkComponent = new TestSparkComponent()

        component.sqlContext should not be null
        component.sqlContext shouldBe component.spark.sqlContext
      }
    }

    // =========================================================================
    // UNHAPPY PATH TESTS
    // =========================================================================

    describe("unhappy path") {

      it("should throw when no master provided and no existing session") {
        // When master is not provided and there's no existing session,
        // Spark will fail because it doesn't know where to run
        val config: SparkConfig = SparkConfig(
          master = None,
          appName = Some("NoMaster")
        )

        // In test context without spark-submit, this should throw
        an[Exception] should be thrownBy {
          SparkSessionWrapper.configure(config)
        }
      }

      it("should handle empty config map gracefully") {
        val config: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("EmptyConfig"),
          config = Map.empty
        )

        val session: SparkSession = SparkSessionWrapper.configure(config)

        session should not be null
      }
    }

    // =========================================================================
    // EDGE CASE TESTS
    // =========================================================================

    describe("edge cases") {

      it("should handle reset correctly") {
        val config1: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("First")
        )
        SparkSessionWrapper.configure(config1)

        SparkSessionWrapper.reset()

        // After reset, configure should create new session
        val config2: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("Second")
        )
        val session2: SparkSession = SparkSessionWrapper.configure(config2)

        // They should be different instances (or same if Spark reuses)
        // The key is that reset clears our internal reference
        session2 should not be null
      }

      it("should handle stop correctly") {
        val config: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("WillStop")
        )
        SparkSessionWrapper.configure(config)

        SparkSessionWrapper.stop()

        // After stop, internal reference should be cleared
        // Next configure will create new session
        val config2: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("AfterStop")
        )
        val newSession: SparkSession = SparkSessionWrapper.configure(config2)
        newSession should not be null
      }

      it("should handle concurrent access to getOrCreate") {
        import scala.concurrent.{Future, Await}
        import scala.concurrent.duration._
        import scala.concurrent.ExecutionContext.Implicits.global

        // First configure a session
        val config: SparkConfig = SparkConfig(master = Some("local[1]"), appName = Some("ConcurrentTest"))
        SparkSessionWrapper.configure(config)

        val futures: Seq[Future[SparkSession]] = (1 to 10).map { _: Int =>
          Future {
            SparkSessionWrapper.getOrCreate()
          }
        }

        val sessions: Seq[SparkSession] = Await.result(Future.sequence(futures), 30.seconds)

        // All futures should return the same session instance
        sessions.distinct should have size 1
      }

      it("should handle concurrent access to configure") {
        import scala.concurrent.{Future, Await}
        import scala.concurrent.duration._
        import scala.concurrent.ExecutionContext.Implicits.global

        // Multiple threads calling configure with different configs
        val futures: Seq[Future[SparkSession]] = (1 to 10).map { i: Int =>
          Future {
            val config: SparkConfig = SparkConfig(
              master = Some("local[1]"),
              appName = Some(s"ConcurrentConfig$i")
            )
            SparkSessionWrapper.configure(config)
          }
        }

        val sessions: Seq[SparkSession] = Await.result(Future.sequence(futures), 30.seconds)

        // All should return the same session instance (Spark reuses active session)
        sessions.distinct should have size 1
      }

      it("should handle config with many Spark properties") {
        val config: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("ManyProps"),
          config = Map(
            "spark.ui.enabled"             -> "false",
            "spark.sql.shuffle.partitions" -> "2",
            "spark.default.parallelism"    -> "2",
            "spark.sql.adaptive.enabled"   -> "false",
            "spark.driver.host"            -> "localhost"
          )
        )

        val session: SparkSession = SparkSessionWrapper.configure(config)

        session.conf.get("spark.sql.shuffle.partitions") shouldBe "2"
        session.conf.get("spark.default.parallelism") shouldBe "2"
      }

      it("should handle reconfiguration attempt") {
        val config1: SparkConfig = SparkConfig(
          master = Some("local[1]"),
          appName = Some("First")
        )
        val session1: SparkSession = SparkSessionWrapper.configure(config1)

        val config2: SparkConfig = SparkConfig(
          master = Some("local[2]"),
          appName = Some("Second")
        )
        val session2: SparkSession = SparkSessionWrapper.configure(config2)

        // Spark's getOrCreate returns existing active session - it cannot be
        // reconfigured without stopping first. Both references point to same session.
        session1 shouldBe theSameInstanceAs(session2)
        // Original config values are retained (not overwritten)
        session2.sparkContext.appName shouldBe "First"
      }
    }
  }
}

// Test fixture
class TestSparkComponent extends SparkSessionWrapper {
  // Exposes trait members for testing
}
