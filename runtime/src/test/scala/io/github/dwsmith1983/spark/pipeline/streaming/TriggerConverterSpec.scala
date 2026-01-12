package io.github.dwsmith1983.spark.pipeline.streaming

import io.github.dwsmith1983.spark.pipeline.config.SparkConfig
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/** Tests for TriggerConverter. */
class TriggerConverterSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val config = SparkConfig(
      master = Some("local[2]"),
      appName = Some("TriggerConverterTest"),
      config = Map(
        "spark.ui.enabled"             -> "false",
        "spark.sql.shuffle.partitions" -> "2"
      )
    )
    spark = SparkSessionWrapper.configure(config)
  }

  override def afterAll(): Unit =
    SparkSessionWrapper.stop()

  describe("TriggerConverter") {

    describe("toSparkTrigger") {

      it("should convert ProcessingTime trigger") {
        val config  = TriggerConfig.ProcessingTime("10 seconds")
        val trigger = TriggerConverter.toSparkTrigger(config)

        trigger shouldBe a[Trigger]
        trigger.toString should include("ProcessingTime")
      }

      it("should convert Once trigger") {
        val config  = TriggerConfig.Once
        val trigger = TriggerConverter.toSparkTrigger(config)

        trigger shouldBe a[Trigger]
        // Spark returns "OneTimeTrigger" for Once
        trigger.toString should (include("Once").or(include("OneTime")))
      }

      it("should convert Continuous trigger") {
        val config  = TriggerConfig.Continuous("1 second")
        val trigger = TriggerConverter.toSparkTrigger(config)

        trigger shouldBe a[Trigger]
        trigger.toString should include("Continuous")
      }

      it("should convert AvailableNow trigger") {
        val config  = TriggerConfig.AvailableNow
        val trigger = TriggerConverter.toSparkTrigger(config)

        trigger shouldBe a[Trigger]
        trigger.toString should include("AvailableNow")
      }
    }
  }
}
