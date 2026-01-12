package io.github.dwsmith1983.spark.pipeline.dq

import com.typesafe.config.ConfigFactory
import io.github.dwsmith1983.spark.pipeline.config._
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Tests for DataQualityHooks. */
class DataQualityHooksSpec
  extends AnyFunSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val config = SparkConfig(
      master = Some("local[2]"),
      appName = Some("DataQualityHooksTest"),
      config = Map(
        "spark.ui.enabled"             -> "false",
        "spark.sql.shuffle.partitions" -> "2"
      )
    )
    spark = SparkSessionWrapper.configure(config)
  }

  override def afterAll(): Unit =
    SparkSessionWrapper.stop()

  override def afterEach(): Unit =
    spark.catalog.listTables().collect().foreach { t =>
      if (t.isTemporary) {
        spark.catalog.dropTempView(t.name)
      }
    }

  // Helper to create test DataFrames
  private def createIntDf(name: String, data: Seq[Int]): Unit = {
    val schema = StructType(Seq(StructField("value", IntegerType, nullable = false)))
    val rows   = data.map(v => Row(v)).asJava
    spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
  }

  // Helper to create test pipeline config
  def pipelineConfig(name: String, components: List[ComponentConfig]): PipelineConfig =
    PipelineConfig(name, components, failFast = true)

  def componentConfig(name: String): ComponentConfig =
    ComponentConfig("test.Class", name, ConfigFactory.empty())

  describe("DataQualityHooks") {

    describe("basic functionality") {

      it("should run checks before pipeline") {
        createIntDf("pre_check_table", Seq(1, 2, 3, 4, 5))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("pre_check_table", minRows = 1),
            CheckTiming.BeforePipeline
          )
        )

        val hooks  = DataQualityHooks(spark, checks, FailureMode.WarnOnly)
        val config = pipelineConfig("test", List.empty)

        hooks.beforePipeline(config)

        hooks.getResults should have size 1
        hooks.getResults.head shouldBe a[DataQualityResult.Passed]
      }

      it("should run checks after specific component") {
        createIntDf("component_table", Seq(1, 2, 3))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("component_table", minRows = 1),
            CheckTiming.AfterComponent("LoadData")
          )
        )

        val hooks      = DataQualityHooks(spark, checks, FailureMode.WarnOnly)
        val config     = pipelineConfig("test", List(componentConfig("LoadData")))
        val compConfig = componentConfig("LoadData")

        hooks.beforePipeline(config)
        hooks.afterComponent(compConfig, 0, 1, 100L)

        hooks.getResults should have size 1
        hooks.allChecksPassed shouldBe true
      }

      it("should run checks after pipeline completion") {
        createIntDf("post_pipeline_table", Seq(1, 2, 3, 4, 5))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("post_pipeline_table", minRows = 5),
            CheckTiming.AfterPipeline
          )
        )

        val hooks  = DataQualityHooks(spark, checks, FailureMode.WarnOnly)
        val config = pipelineConfig("test", List.empty)

        hooks.beforePipeline(config)
        hooks.afterPipeline(config, PipelineResult.Success(100L, 0))

        hooks.getResults should have size 1
        hooks.allChecksPassed shouldBe true
      }

      it("should skip after-pipeline checks when pipeline failed") {
        createIntDf("skip_table", Seq(1))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("skip_table", minRows = 1),
            CheckTiming.AfterPipeline
          )
        )

        val hooks  = DataQualityHooks(spark, checks, FailureMode.WarnOnly)
        val config = pipelineConfig("test", List.empty)

        hooks.beforePipeline(config)
        hooks.afterPipeline(config, PipelineResult.Failure(new Exception("test"), None, 0))

        hooks.getResults shouldBe empty
      }
    }

    describe("failure modes") {

      it("should throw exception in FailOnError mode") {
        createIntDf("fail_table", Seq(1))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("fail_table", minRows = 100),
            CheckTiming.BeforePipeline
          )
        )

        val hooks  = DataQualityHooks(spark, checks, FailureMode.FailOnError)
        val config = pipelineConfig("test", List.empty)

        a[DataQualityException] should be thrownBy {
          hooks.beforePipeline(config)
        }
      }

      it("should not throw in WarnOnly mode") {
        createIntDf("warn_table", Seq(1))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("warn_table", minRows = 100),
            CheckTiming.BeforePipeline
          )
        )

        val hooks  = DataQualityHooks(spark, checks, FailureMode.WarnOnly)
        val config = pipelineConfig("test", List.empty)

        noException should be thrownBy {
          hooks.beforePipeline(config)
        }

        hooks.allChecksPassed shouldBe false
        hooks.getFailedResults should have size 1
      }

      it("should respect threshold in Threshold mode") {
        createIntDf("threshold_table", Seq(1))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("threshold_table", minRows = 100),
            CheckTiming.BeforePipeline
          ),
          DataQualityCheckConfig(
            RowCountCheck("threshold_table", minRows = 200),
            CheckTiming.BeforePipeline
          )
        )

        // Allow up to 2 failures - should not throw
        val hooks2 = DataQualityHooks(spark, checks, FailureMode.Threshold(2))
        val config = pipelineConfig("test", List.empty)

        noException should be thrownBy {
          hooks2.beforePipeline(config)
        }

        hooks2.getFailedResults should have size 2
      }

      it("should throw when threshold exceeded") {
        createIntDf("threshold_exceed_table", Seq(1))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("threshold_exceed_table", minRows = 100),
            CheckTiming.BeforePipeline
          ),
          DataQualityCheckConfig(
            RowCountCheck("threshold_exceed_table", minRows = 200),
            CheckTiming.BeforePipeline
          ),
          DataQualityCheckConfig(
            RowCountCheck("threshold_exceed_table", minRows = 300),
            CheckTiming.BeforePipeline
          )
        )

        // Allow only 2 failures - third should throw
        val hooks  = DataQualityHooks(spark, checks, FailureMode.Threshold(2))
        val config = pipelineConfig("test", List.empty)

        a[DataQualityException] should be thrownBy {
          hooks.beforePipeline(config)
        }
      }
    }

    describe("result collection") {

      it("should collect all results") {
        createIntDf("multi_check_table", Seq(1, 2, 3))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("multi_check_table", minRows = 1),
            CheckTiming.BeforePipeline
          ),
          DataQualityCheckConfig(
            NullCheck("multi_check_table", Seq("value"), maxNullPercent = 100.0),
            CheckTiming.BeforePipeline
          )
        )

        val hooks  = DataQualityHooks(spark, checks, FailureMode.WarnOnly)
        val config = pipelineConfig("test", List.empty)

        hooks.beforePipeline(config)

        hooks.getResults should have size 2
        hooks.allChecksPassed shouldBe true
      }

      it("should track failed results separately") {
        createIntDf("mixed_results_table", Seq(1))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("mixed_results_table", minRows = 1), // passes
            CheckTiming.BeforePipeline
          ),
          DataQualityCheckConfig(
            RowCountCheck("mixed_results_table", minRows = 100), // fails
            CheckTiming.BeforePipeline
          )
        )

        val hooks  = DataQualityHooks(spark, checks, FailureMode.WarnOnly)
        val config = pipelineConfig("test", List.empty)

        hooks.beforePipeline(config)

        hooks.getResults should have size 2
        hooks.getFailedResults should have size 1
        hooks.allChecksPassed shouldBe false
      }

      it("should reset results between pipeline runs") {
        createIntDf("reset_table", Seq(1, 2, 3))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("reset_table", minRows = 1),
            CheckTiming.BeforePipeline
          )
        )

        val hooks   = DataQualityHooks(spark, checks, FailureMode.WarnOnly)
        val config1 = pipelineConfig("test1", List.empty)
        val config2 = pipelineConfig("test2", List.empty)

        hooks.beforePipeline(config1)
        hooks.getResults should have size 1

        hooks.beforePipeline(config2)
        hooks.getResults should have size 1 // Reset, not accumulated
      }
    }

    describe("sink integration") {

      it("should write results to sink") {
        createIntDf("sink_table", Seq(1, 2, 3))

        val writtenResults = mutable.ListBuffer[DataQualityResult]()
        val testSink = new DataQualitySink {
          override def write(result: DataQualityResult): Unit = writtenResults += result
          override def flush(): Unit                          = ()
          override def close(): Unit                          = ()
        }

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("sink_table", minRows = 1),
            CheckTiming.BeforePipeline
          )
        )

        val hooks  = DataQualityHooks(spark, checks, FailureMode.WarnOnly, testSink)
        val config = pipelineConfig("test", List.empty)

        hooks.beforePipeline(config)

        writtenResults should have size 1
      }
    }

    describe("component name matching") {

      it("should only run checks for matching component") {
        createIntDf("component_match_table", Seq(1, 2, 3))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("component_match_table", minRows = 1),
            CheckTiming.AfterComponent("StepA")
          ),
          DataQualityCheckConfig(
            RowCountCheck("component_match_table", minRows = 1),
            CheckTiming.AfterComponent("StepB")
          )
        )

        val hooks      = DataQualityHooks(spark, checks, FailureMode.WarnOnly)
        val config     = pipelineConfig("test", List(componentConfig("StepA"), componentConfig("StepB")))
        val compConfig = componentConfig("StepA")

        hooks.beforePipeline(config)
        hooks.afterComponent(compConfig, 0, 2, 100L)

        hooks.getResults should have size 1 // Only StepA check
      }
    }

    describe("factory methods") {

      it("should create hooks with toFile") {
        createIntDf("factory_table", Seq(1))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("factory_table", minRows = 1),
            CheckTiming.BeforePipeline
          )
        )

        val tempFile = java.io.File.createTempFile("dq-test", ".jsonl")
        tempFile.deleteOnExit()

        val hooks  = DataQualityHooks.toFile(spark, checks, tempFile.getAbsolutePath)
        val config = pipelineConfig("test", List.empty)

        hooks.beforePipeline(config)
        hooks.afterPipeline(config, PipelineResult.Success(100L, 0))

        val content = scala.io.Source.fromFile(tempFile).mkString
        content should include("passed")
      }

      it("should create warn-only hooks") {
        createIntDf("warn_only_table", Seq(1))

        val checks = Seq(
          DataQualityCheckConfig(
            RowCountCheck("warn_only_table", minRows = 100),
            CheckTiming.BeforePipeline
          )
        )

        val hooks  = DataQualityHooks.warnOnly(spark, checks)
        val config = pipelineConfig("test", List.empty)

        noException should be thrownBy {
          hooks.beforePipeline(config)
        }
      }
    }
  }
}
