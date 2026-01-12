package io.github.dwsmith1983.spark.pipeline.streaming

import io.github.dwsmith1983.spark.pipeline.config.SparkConfig
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{Files, Path}

/** Tests for StreamingSource, StreamingSink, and StreamingPipeline. */
class StreamingPipelineSpec
  extends AnyFunSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: Path       = _

  override def beforeAll(): Unit = {
    val config = SparkConfig(
      master = Some("local[2]"),
      appName = Some("StreamingPipelineTest"),
      config = Map(
        "spark.ui.enabled"             -> "false",
        "spark.sql.shuffle.partitions" -> "2"
      )
    )
    spark = SparkSessionWrapper.configure(config)
  }

  override def afterAll(): Unit =
    SparkSessionWrapper.stop()

  override def beforeEach(): Unit =
    tempDir = Files.createTempDirectory("streaming-test")

  override def afterEach(): Unit = {
    import scala.reflect.io.Directory
    new Directory(tempDir.toFile).deleteRecursively()
  }

  describe("StreamingSource") {

    it("should throw UnsupportedOperationException when run() is called") {
      val source = new StreamingSource {
        override def readStream(): DataFrame = spark.emptyDataFrame
      }

      val exception = intercept[UnsupportedOperationException] {
        source.run()
      }

      exception.getMessage should include("StreamingSource.run()")
      exception.getMessage should include("StreamingPipeline")
    }

    it("should provide default None for watermark configuration") {
      val source = new StreamingSource {
        override def readStream(): DataFrame = spark.emptyDataFrame
      }

      source.watermarkColumn shouldBe None
      source.watermarkDelay shouldBe None
    }

    it("should allow overriding watermark configuration") {
      val source = new StreamingSource {
        override def readStream(): DataFrame         = spark.emptyDataFrame
        override def watermarkColumn: Option[String] = Some("event_time")
        override def watermarkDelay: Option[String]  = Some("10 minutes")
      }

      source.watermarkColumn shouldBe Some("event_time")
      source.watermarkDelay shouldBe Some("10 minutes")
    }
  }

  describe("StreamingSink") {

    it("should throw UnsupportedOperationException when run() is called") {
      val sink = new StreamingSink {
        override def writeStream(df: DataFrame): DataStreamWriter[Row] =
          df.writeStream.format("console")
        override def outputMode: OutputMode     = OutputMode.Append()
        override def checkpointLocation: String = tempDir.resolve("checkpoint").toString
      }

      val exception = intercept[UnsupportedOperationException] {
        sink.run()
      }

      exception.getMessage should include("StreamingSink.run()")
      exception.getMessage should include("StreamingPipeline")
    }

    it("should provide default None for queryName") {
      val sink = new StreamingSink {
        override def writeStream(df: DataFrame): DataStreamWriter[Row] =
          df.writeStream.format("console")
        override def outputMode: OutputMode     = OutputMode.Append()
        override def checkpointLocation: String = tempDir.resolve("checkpoint").toString
      }

      sink.queryName shouldBe None
    }
  }

  describe("StreamingPipeline") {

    it("should start and stop a streaming query using rate source") {
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val pipeline = new StreamingPipeline {
        override def source: StreamingSource = new StreamingSource {
          override def readStream(): DataFrame =
            spark.readStream
              .format("rate")
              .option("rowsPerSecond", "100")
              .load()
        }

        override def sink: StreamingSink = new StreamingSink {
          override def writeStream(df: DataFrame): DataStreamWriter[Row] =
            df.writeStream.format("memory")

          override def outputMode: OutputMode     = OutputMode.Append()
          override def checkpointLocation: String = checkpointPath
          override def queryName: Option[String]  = Some("rate_test_output")
        }

        override def trigger: TriggerConfig = TriggerConfig.Once
      }

      val query = pipeline.startStream()

      try {
        // Once trigger auto-terminates, so await completion
        query.awaitTermination()

        // Verify the query completed successfully and table exists with correct schema
        // Note: Once trigger may complete before rate source generates data,
        // so we only verify schema, not row count
        val result = spark.table("rate_test_output")
        (result.schema.fieldNames should contain).allOf("timestamp", "value")
      } finally {
        if (query.isActive) query.stop()
      }
    }

    it("should apply transformations") {
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val pipeline = new StreamingPipeline {
        override def source: StreamingSource = new StreamingSource {
          override def readStream(): DataFrame =
            spark.readStream
              .format("rate")
              .option("rowsPerSecond", "100")
              .load()
        }

        override def sink: StreamingSink = new StreamingSink {
          override def writeStream(df: DataFrame): DataStreamWriter[Row] =
            df.writeStream.format("memory").queryName("transformed_rate_output")

          override def outputMode: OutputMode     = OutputMode.Append()
          override def checkpointLocation: String = checkpointPath
        }

        override def transform(df: DataFrame): DataFrame =
          // Filter to only keep even values
          df.filter(df("value") % 2 === 0)

        override def trigger: TriggerConfig = TriggerConfig.Once
      }

      val query = pipeline.startStream()

      try {
        // Once trigger auto-terminates, so await completion
        query.awaitTermination()

        val result = spark.table("transformed_rate_output")
        // All values should be even
        val values = result.select("value").collect().map(_.getLong(0))
        values.foreach(_ % 2 shouldBe 0)
      } finally {
        if (query.isActive) query.stop()
      }
    }

    it("should use default identity transformation when not overridden") {
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val pipeline = new StreamingPipeline {
        override def source: StreamingSource = new StreamingSource {
          override def readStream(): DataFrame =
            spark.readStream
              .format("rate")
              .option("rowsPerSecond", "50")
              .load()
        }

        override def sink: StreamingSink = new StreamingSink {
          override def writeStream(df: DataFrame): DataStreamWriter[Row] =
            df.writeStream.format("memory").queryName("identity_rate_output")

          override def outputMode: OutputMode     = OutputMode.Append()
          override def checkpointLocation: String = checkpointPath
        }

        override def trigger: TriggerConfig = TriggerConfig.Once
      }

      val query = pipeline.startStream()

      try {
        // Once trigger auto-terminates, so await completion
        query.awaitTermination()

        val result = spark.table("identity_rate_output")
        // Should have both timestamp and value columns from rate source
        (result.schema.fieldNames should contain).allOf("timestamp", "value")
      } finally {
        if (query.isActive) query.stop()
      }
    }

    it("should use default ProcessingTime trigger") {
      val pipeline = new StreamingPipeline {
        override def source: StreamingSource = new StreamingSource {
          override def readStream(): DataFrame = spark.emptyDataFrame
        }
        override def sink: StreamingSink = new StreamingSink {
          override def writeStream(df: DataFrame): DataStreamWriter[Row] =
            df.writeStream.format("console")
          override def outputMode: OutputMode     = OutputMode.Append()
          override def checkpointLocation: String = "/tmp/test"
        }
      }

      pipeline.trigger shouldBe TriggerConfig.ProcessingTime("0 seconds")
    }
  }
}
