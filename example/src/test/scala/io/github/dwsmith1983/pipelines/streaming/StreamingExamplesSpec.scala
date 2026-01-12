package io.github.dwsmith1983.pipelines.streaming

import com.typesafe.config.ConfigFactory
import io.github.dwsmith1983.spark.pipeline.config.SparkConfig
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import io.github.dwsmith1983.spark.pipeline.streaming.{StreamingPipeline, TriggerConfig}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{Files, Path}

/** Tests for example streaming implementations. */
class StreamingExamplesSpec
  extends AnyFunSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: Path       = _

  override def beforeAll(): Unit = {
    val config = SparkConfig(
      master = Some("local[2]"),
      appName = Some("StreamingExamplesTest"),
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
    tempDir = Files.createTempDirectory("streaming-examples-test")

  override def afterEach(): Unit = {
    import scala.reflect.io.Directory
    new Directory(tempDir.toFile).deleteRecursively()
  }

  describe("RateStreamSource") {

    it("should create from config") {
      val hocon = ConfigFactory.parseString("""
        rows-per-second = 5
        ramp-up-time-seconds = 0
        num-partitions = 1
        enable-watermark = false
        watermark-delay = "10 seconds"
      """)

      val source = RateStreamSource.createFromConfig(hocon).asInstanceOf[RateStreamSource]
      source should not be null
    }

    it("should generate streaming DataFrame") {
      val conf = RateStreamSource.Config(
        rowsPerSecond = 10,
        rampUpTimeSeconds = 0,
        numPartitions = 1
      )

      val source = new RateStreamSource(conf)
      val df     = source.readStream()

      df.isStreaming shouldBe true
      (df.schema.fieldNames should contain).allOf("timestamp", "value")
    }

    it("should disable watermark by default") {
      val conf   = RateStreamSource.Config()
      val source = new RateStreamSource(conf)

      source.watermarkColumn shouldBe None
      source.watermarkDelay shouldBe None
    }

    it("should enable watermark when configured") {
      val conf = RateStreamSource.Config(
        enableWatermark = true,
        watermarkDelay = "30 seconds"
      )
      val source = new RateStreamSource(conf)

      source.watermarkColumn shouldBe Some("timestamp")
      source.watermarkDelay shouldBe Some("30 seconds")
    }
  }

  describe("ConsoleStreamSink") {

    it("should create from config") {
      val hocon = ConfigFactory.parseString(s"""
        checkpoint-location = "${tempDir.resolve("checkpoint").toString.replace("\\", "\\\\")}"
        output-mode = "append"
        num-rows = 10
        truncate = false
        query-name = "test-console"
      """)

      val sink = ConsoleStreamSink.createFromConfig(hocon).asInstanceOf[ConsoleStreamSink]
      sink should not be null
    }

    it("should support append output mode") {
      val conf = ConsoleStreamSink.Config(
        checkpointLocation = tempDir.resolve("checkpoint").toString,
        outputMode = "append"
      )

      val sink = new ConsoleStreamSink(conf)
      sink.outputMode shouldBe OutputMode.Append()
    }

    it("should support complete output mode") {
      val conf = ConsoleStreamSink.Config(
        checkpointLocation = tempDir.resolve("checkpoint").toString,
        outputMode = "complete"
      )

      val sink = new ConsoleStreamSink(conf)
      sink.outputMode shouldBe OutputMode.Complete()
    }

    it("should support update output mode") {
      val conf = ConsoleStreamSink.Config(
        checkpointLocation = tempDir.resolve("checkpoint").toString,
        outputMode = "update"
      )

      val sink = new ConsoleStreamSink(conf)
      sink.outputMode shouldBe OutputMode.Update()
    }

    it("should throw for invalid output mode") {
      val conf = ConsoleStreamSink.Config(
        checkpointLocation = tempDir.resolve("checkpoint").toString,
        outputMode = "invalid"
      )

      val sink = new ConsoleStreamSink(conf)

      val exception = intercept[IllegalArgumentException] {
        sink.outputMode
      }

      exception.getMessage should include("invalid")
    }

    it("should provide checkpoint location") {
      val checkpointPath = tempDir.resolve("my-checkpoint").toString
      val conf = ConsoleStreamSink.Config(
        checkpointLocation = checkpointPath
      )

      val sink = new ConsoleStreamSink(conf)
      sink.checkpointLocation shouldBe checkpointPath
    }

    it("should provide query name when configured") {
      val conf = ConsoleStreamSink.Config(
        checkpointLocation = tempDir.resolve("checkpoint").toString,
        queryName = Some("my-query")
      )

      val sink = new ConsoleStreamSink(conf)
      sink.queryName shouldBe Some("my-query")
    }
  }

  describe("End-to-end streaming pipeline") {

    it("should run rate source to memory sink") {
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val rateSource = new RateStreamSource(RateStreamSource.Config(
        rowsPerSecond = 100,
        numPartitions = 1
      ))

      val pipeline = new StreamingPipeline {
        override def source = rateSource

        override def sink = new ConsoleStreamSink(ConsoleStreamSink.Config(
          checkpointLocation = checkpointPath,
          outputMode = "append"
        )) {
          // Override to use memory sink for testing
          override def writeStream(df: DataFrame): DataStreamWriter[Row] =
            df.writeStream.format("memory").queryName("e2e_test")
        }

        override def trigger = TriggerConfig.Once
      }

      val query = pipeline.startStream()

      try {
        // Once trigger auto-terminates, so await completion
        query.awaitTermination()

        // Verify the pipeline completed and table exists with correct schema
        // Note: Once trigger may complete before rate source generates data,
        // so we only verify schema, not row count
        val result = spark.table("e2e_test")
        (result.schema.fieldNames should contain).allOf("timestamp", "value")
      } finally {
        if (query.isActive) query.stop()
      }
    }
  }
}
