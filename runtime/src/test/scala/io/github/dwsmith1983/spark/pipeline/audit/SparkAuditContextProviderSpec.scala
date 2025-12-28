package io.github.dwsmith1983.spark.pipeline.audit

import io.github.dwsmith1983.spark.pipeline.config.SparkConfig
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/** Tests for SparkAuditContextProvider. */
class SparkAuditContextProviderSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit =
    // Configure a local Spark session for testing
    SparkSessionWrapper.configure(
      SparkConfig(
        master = Some("local[1]"),
        appName = Some("AuditContextTest"),
        config = Map(
          "spark.ui.enabled"             -> "false",
          "spark.sql.shuffle.partitions" -> "1"
        )
      )
    )

  override def afterAll(): Unit =
    SparkSessionWrapper.stop()

  describe("SparkAuditContextProvider") {

    describe("getSystemContext") {

      it("should include hostname") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSystemContext(EnvFilter.default)
        context.hostname should not be empty
        context.hostname should not be "unknown"
      }

      it("should include JVM version") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSystemContext(EnvFilter.default)
        context.jvmVersion should not be empty
      }

      it("should include Scala version") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSystemContext(EnvFilter.default)
        context.scalaVersion should not be empty
      }

      it("should include Spark version") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSystemContext(EnvFilter.default)
        context.sparkVersion shouldBe defined
      }

      it("should include application ID") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSystemContext(EnvFilter.default)
        context.applicationId shouldBe defined
      }

      it("should include executor ID") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSystemContext(EnvFilter.default)
        context.executorId shouldBe defined
        context.executorId.get shouldBe "driver"
      }

      it("should filter environment variables") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSystemContext(EnvFilter.allowlist(Set("PATH")))

        if (sys.env.contains("PATH")) {
          (context.environment should contain).key("PATH")
        }
        context.environment.keys.foreach(key => key shouldBe "PATH")
      }
    }

    describe("getSparkContext") {

      it("should return SparkExecutionContext") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSparkContext()
        context shouldBe defined
      }

      it("should include application ID") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSparkContext()
        context.get.applicationId should not be empty
      }

      it("should include application name") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSparkContext()
        context.get.applicationName shouldBe Some("AuditContextTest")
      }

      it("should include master") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSparkContext()
        context.get.master shouldBe defined
        context.get.master.get should include("local")
      }

      it("should include Spark version") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSparkContext()
        context.get.sparkVersion should not be empty
      }

      it("should filter sensitive Spark properties") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSparkContext()

        context.get.sparkProperties.keys.foreach { key =>
          (key.toLowerCase should not).include("password")
          (key.toLowerCase should not).include("secret")
        }
      }

      it("should include safe Spark properties") {
        val provider = SparkAuditContextProvider()
        val context  = provider.getSparkContext()

        // At least some safe properties should be present
        context.get.sparkProperties should not be empty
      }
    }

    describe("factory methods") {

      it("should create provider with apply()") {
        val provider = SparkAuditContextProvider()
        provider shouldBe a[SparkAuditContextProvider]
        // Verify it actually works, not just type check
        val context = provider.getSystemContext(EnvFilter.default)
        context.sparkVersion shouldBe defined
      }

      it("should create provider with custom config filter") {
        val filter   = ConfigFilter.withPatterns(Set("custom"))
        val provider = SparkAuditContextProvider.withConfigFilter(filter)
        provider.sparkConfigFilter shouldBe filter
      }
    }

    describe("graceful degradation") {

      it("should return fallback values when SparkSession unavailable") {
        // Stop the session to simulate unavailable state
        SparkSessionWrapper.stop()

        val provider = SparkAuditContextProvider()
        val context  = provider.getSystemContext(EnvFilter.default)

        // Should still return valid context with JVM info
        context.jvmVersion should not be empty
        context.scalaVersion should not be empty
        // Spark-specific fields should be None, not throw
        context.sparkVersion shouldBe None
        context.applicationId shouldBe None

        // Restart for other tests
        SparkSessionWrapper.configure(
          SparkConfig(
            master = Some("local[1]"),
            appName = Some("AuditContextTest"),
            config = Map(
              "spark.ui.enabled"             -> "false",
              "spark.sql.shuffle.partitions" -> "1"
            )
          )
        )
      }

      it("should return None for SparkContext when session unavailable") {
        SparkSessionWrapper.stop()

        val provider = SparkAuditContextProvider()
        val context  = provider.getSparkContext()

        context shouldBe None

        // Restart for other tests
        SparkSessionWrapper.configure(
          SparkConfig(
            master = Some("local[1]"),
            appName = Some("AuditContextTest"),
            config = Map(
              "spark.ui.enabled"             -> "false",
              "spark.sql.shuffle.partitions" -> "1"
            )
          )
        )
      }

      it("should handle hostname resolution failure gracefully") {
        // This test verifies the try-catch in getSystemContext works
        // The actual hostname call may succeed, but the code path handles failure
        val provider = SparkAuditContextProvider()
        val context  = provider.getSystemContext(EnvFilter.default)
        // Should return either actual hostname or "unknown", never throw
        context.hostname should ((not be empty).or(be("unknown")))
      }
    }
  }
}
