package io.github.dwsmith1983.pipelines

import io.github.dwsmith1983.spark.pipeline.config._
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner
import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll

import java.nio.file.{Files, Path}

/**
 * Tests for MetricsHooks example implementation.
 *
 * Demonstrates practical usage of PipelineHooks for metrics collection.
 */
class MetricsHooksSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  var tempDir: Path   = _
  var inputFile: Path = _
  var outputDir: Path = _

  override def beforeEach(): Unit = {
    SparkSessionWrapper.stop()

    // Create temp directories for test data
    tempDir = Files.createTempDirectory("metrics-hooks-test")
    inputFile = tempDir.resolve("input.txt")
    outputDir = tempDir.resolve("output")

    // Create sample input
    val _: Path = Files.writeString(inputFile, "hello world hello spark world world")
  }

  override def afterEach(): Unit = {
    SparkSessionWrapper.stop()
    // Clean up temp files
    if (tempDir != null) {
      Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder())
        .forEach { (p: Path) =>
          val _: Boolean = Files.deleteIfExists(p)
        }
    }
  }

  override def afterAll(): Unit =
    SparkSessionWrapper.stop()

  describe("MetricsHooks") {

    it("should collect metrics for successful pipeline") {
      val config = ConfigFactory.parseString(s"""
        spark {
          master = "local[1]"
          app-name = "MetricsTest"
        }
        pipeline {
          pipeline-name = "Word Count Pipeline"
          pipeline-components = [
            {
              instance-type = "io.github.dwsmith1983.pipelines.WordCount"
              instance-name = "WordCount(test)"
              instance-config {
                input-path = "${inputFile.toString.replace("\\", "/")}"
                output-path = "${outputDir.toString.replace("\\", "/")}"
                min-count = 1
              }
            }
          ]
        }
      """)

      val metrics = MetricsHooks()

      SimplePipelineRunner.run(config, metrics)

      // Verify metrics were collected
      metrics.pipelineName shouldBe "Word Count Pipeline"
      metrics.wasSuccessful shouldBe true
      metrics.componentCount shouldBe 1
      metrics.successCount shouldBe 1
      metrics.failureCount shouldBe 0
      metrics.totalDurationMs should be > 0L
      (metrics.componentDurations should contain).key("WordCount(test)")
      metrics.failedComponent shouldBe None
    }

    it("should generate readable report") {
      val config = ConfigFactory.parseString(s"""
        spark {
          master = "local[1]"
          app-name = "ReportTest"
        }
        pipeline {
          pipeline-name = "Report Test Pipeline"
          pipeline-components = [
            {
              instance-type = "io.github.dwsmith1983.pipelines.WordCount"
              instance-name = "WordCount(report)"
              instance-config {
                input-path = "${inputFile.toString.replace("\\", "/")}"
                output-path = "${outputDir.toString.replace("\\", "/")}"
              }
            }
          ]
        }
      """)

      val metrics = MetricsHooks()
      SimplePipelineRunner.run(config, metrics)

      val report = metrics.generateReport()

      report should include("Pipeline: Report Test Pipeline")
      report should include("Status: SUCCESS")
      report should include("Total Duration:")
      report should include("Components: 1/1 completed")
      report should include("WordCount(report):")
    }

    it("should track failure information") {
      val config = ConfigFactory.parseString(s"""
        spark {
          master = "local[1]"
        }
        pipeline {
          pipeline-name = "Failure Test"
          pipeline-components = [
            {
              instance-type = "io.github.dwsmith1983.pipelines.WordCount"
              instance-name = "WillFail"
              instance-config {
                input-path = "/nonexistent/path/that/does/not/exist"
                output-path = "${outputDir.toString.replace("\\", "/")}"
              }
            }
          ]
        }
      """)

      val metrics = MetricsHooks()

      intercept[Exception] {
        SimplePipelineRunner.run(config, metrics)
      }

      metrics.wasSuccessful shouldBe false
      metrics.failureCount shouldBe 1
      metrics.failedComponent shouldBe Some("WillFail")
      metrics.failureError shouldBe defined
    }

    it("should support reset for reuse") {
      val metrics = MetricsHooks()

      // Simulate some state
      val config = ConfigFactory.parseString(s"""
        spark {
          master = "local[1]"
        }
        pipeline {
          pipeline-name = "Reset Test"
          pipeline-components = [
            {
              instance-type = "io.github.dwsmith1983.pipelines.WordCount"
              instance-name = "WordCount(reset)"
              instance-config {
                input-path = "${inputFile.toString.replace("\\", "/")}"
                output-path = "${outputDir.toString.replace("\\", "/")}"
              }
            }
          ]
        }
      """)

      SimplePipelineRunner.run(config, metrics)
      metrics.successCount should be > 0

      // Reset and verify clean state
      metrics.reset()

      metrics.pipelineName shouldBe null
      metrics.totalDurationMs shouldBe 0L
      metrics.componentCount shouldBe 0
      metrics.successCount shouldBe 0
      metrics.failureCount shouldBe 0
      metrics.componentDurations shouldBe empty
    }

    it("should work with PipelineHooks.compose") {
      val config = ConfigFactory.parseString(s"""
        spark {
          master = "local[1]"
        }
        pipeline {
          pipeline-name = "Compose Test"
          pipeline-components = [
            {
              instance-type = "io.github.dwsmith1983.pipelines.WordCount"
              instance-name = "WordCount(compose)"
              instance-config {
                input-path = "${inputFile.toString.replace("\\", "/")}"
                output-path = "${outputDir.toString.replace("\\", "/")}"
              }
            }
          ]
        }
      """)

      val metrics1 = MetricsHooks()
      val metrics2 = MetricsHooks()
      val composed = PipelineHooks.compose(metrics1, metrics2)

      SimplePipelineRunner.run(config, composed)

      // Both hooks should have collected the same metrics
      metrics1.successCount shouldBe 1
      metrics2.successCount shouldBe 1
      metrics1.pipelineName shouldBe metrics2.pipelineName
    }
  }
}
