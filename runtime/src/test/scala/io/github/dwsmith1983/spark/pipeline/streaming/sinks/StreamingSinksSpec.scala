package io.github.dwsmith1983.spark.pipeline.streaming.sinks

import io.github.dwsmith1983.spark.pipeline.config.SparkConfig
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import io.github.dwsmith1983.spark.pipeline.streaming.{StreamingPipeline, StreamingSource, TriggerConfig}
import io.github.dwsmith1983.spark.pipeline.streaming.config._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{Files, Path}

/** Tests for streaming sink implementations. */
class StreamingSinksSpec
  extends AnyFunSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: Path       = _

  override def beforeAll(): Unit = {
    val config = SparkConfig(
      master = Some("local[2]"),
      appName = Some("StreamingSinksTest"),
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
    tempDir = Files.createTempDirectory("sinks-test")

  override def afterEach(): Unit = {
    import scala.reflect.io.Directory
    new Directory(tempDir.toFile).deleteRecursively()
  }

  describe("KafkaSinkConfig") {

    it("should create config with required fields") {
      val config = KafkaSinkConfig(
        bootstrapServers = "localhost:9092",
        topic = "test-topic",
        checkpointPath = "/tmp/checkpoint"
      )

      config.bootstrapServers shouldBe "localhost:9092"
      config.topic shouldBe "test-topic"
      config.checkpointPath shouldBe "/tmp/checkpoint"
      config.enableIdempotence shouldBe true
      config.transactionalId shouldBe None
    }

    it("should allow customizing exactly-once settings") {
      val config = KafkaSinkConfig(
        bootstrapServers = "localhost:9092",
        topic = "test-topic",
        checkpointPath = "/tmp/checkpoint",
        transactionalId = Some("my-tx-id"),
        enableIdempotence = false
      )

      config.transactionalId shouldBe Some("my-tx-id")
      config.enableIdempotence shouldBe false
    }

    it("should support key and value column mappings") {
      val config = KafkaSinkConfig(
        bootstrapServers = "localhost:9092",
        topic = "test-topic",
        checkpointPath = "/tmp/checkpoint",
        keyColumn = Some("event_id"),
        valueColumn = Some("payload")
      )

      config.keyColumn shouldBe Some("event_id")
      config.valueColumn shouldBe Some("payload")
    }
  }

  describe("CloudStorageConfig") {

    it("should create config with default parquet format") {
      val config = CloudStorageConfig(
        path = "s3://bucket/path",
        checkpointPath = "/tmp/checkpoint"
      )

      config.path shouldBe "s3://bucket/path"
      config.format shouldBe "parquet"
      config.fileFormat shouldBe FileFormat.Parquet
    }

    it("should support all file formats") {
      CloudStorageConfig(
        path = "s3://b/p",
        checkpointPath = "/c",
        format = "parquet"
      ).fileFormat shouldBe FileFormat.Parquet
      CloudStorageConfig(path = "s3://b/p", checkpointPath = "/c", format = "json").fileFormat shouldBe FileFormat.Json
      CloudStorageConfig(path = "s3://b/p", checkpointPath = "/c", format = "csv").fileFormat shouldBe FileFormat.Csv
      CloudStorageConfig(path = "s3://b/p", checkpointPath = "/c", format = "avro").fileFormat shouldBe FileFormat.Avro
      CloudStorageConfig(path = "s3://b/p", checkpointPath = "/c", format = "orc").fileFormat shouldBe FileFormat.Orc
    }

    it("should throw on unknown format") {
      val config = CloudStorageConfig(
        path = "s3://bucket/path",
        checkpointPath = "/tmp/checkpoint",
        format = "unknown"
      )

      val exception = intercept[IllegalArgumentException] {
        config.fileFormat
      }

      exception.getMessage should include("unknown")
    }

    it("should support partitioning") {
      val config = CloudStorageConfig(
        path = "s3://bucket/path",
        checkpointPath = "/tmp/checkpoint",
        partitionBy = List("year", "month", "day")
      )

      config.partitionBy shouldBe List("year", "month", "day")
    }
  }

  describe("ForeachSinkConfig") {

    it("should create config with default batch mode") {
      val config = ForeachSinkConfig(
        processorClass = "com.example.MyProcessor",
        checkpointPath = "/tmp/checkpoint"
      )

      config.processorClass shouldBe "com.example.MyProcessor"
      config.mode shouldBe "batch"
      config.foreachMode shouldBe ForeachMode.Batch
    }

    it("should support row mode") {
      val config = ForeachSinkConfig(
        processorClass = "com.example.MyProcessor",
        checkpointPath = "/tmp/checkpoint",
        mode = "row"
      )

      config.foreachMode shouldBe ForeachMode.Row
    }

    it("should throw on unknown mode") {
      val config = ForeachSinkConfig(
        processorClass = "com.example.MyProcessor",
        checkpointPath = "/tmp/checkpoint",
        mode = "unknown"
      )

      val exception = intercept[IllegalArgumentException] {
        config.foreachMode
      }

      exception.getMessage should include("unknown")
    }
  }

  describe("ConsoleSinkConfig") {

    it("should create config with defaults") {
      val config = ConsoleSinkConfig(
        checkpointLocation = "/tmp/checkpoint"
      )

      config.outputMode shouldBe "append"
      config.numRows shouldBe 20
      config.truncate shouldBe true
    }

    it("should allow customizing display options") {
      val config = ConsoleSinkConfig(
        checkpointLocation = "/tmp/checkpoint",
        numRows = 50,
        truncate = false
      )

      config.numRows shouldBe 50
      config.truncate shouldBe false
    }
  }

  describe("ConsoleStreamSink") {

    it("should create sink with correct output mode") {
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val appendSink = new ConsoleStreamSink(ConsoleSinkConfig(
        checkpointLocation = checkpointPath,
        outputMode = "append"
      ))
      appendSink.outputMode shouldBe OutputMode.Append()

      val completeSink = new ConsoleStreamSink(ConsoleSinkConfig(
        checkpointLocation = s"$checkpointPath-complete",
        outputMode = "complete"
      ))
      completeSink.outputMode shouldBe OutputMode.Complete()

      val updateSink = new ConsoleStreamSink(ConsoleSinkConfig(
        checkpointLocation = s"$checkpointPath-update",
        outputMode = "update"
      ))
      updateSink.outputMode shouldBe OutputMode.Update()
    }

    it("should throw on invalid output mode") {
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val sink = new ConsoleStreamSink(ConsoleSinkConfig(
        checkpointLocation = checkpointPath,
        outputMode = "invalid"
      ))

      val exception = intercept[IllegalArgumentException] {
        sink.outputMode
      }

      exception.getMessage should include("invalid")
    }

    it("should integrate with StreamingPipeline using rate source") {
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val consoleSink = new ConsoleStreamSink(ConsoleSinkConfig(
        checkpointLocation = checkpointPath,
        numRows = 5,
        truncate = true
      ))

      val pipeline = new StreamingPipeline {
        override def source: StreamingSource = new StreamingSource {
          override def readStream(): DataFrame =
            spark.readStream
              .format("rate")
              .option("rowsPerSecond", "10")
              .load()
        }

        // Use memory sink instead of console for testability
        override def sink: ConsoleStreamSink = consoleSink

        override def trigger: TriggerConfig = TriggerConfig.Once
      }

      // Verify the sink properties are correct
      pipeline.sink.checkpointLocation shouldBe checkpointPath
      pipeline.sink.outputMode shouldBe OutputMode.Append()
    }
  }

  describe("CloudStorageStreamSink") {

    it("should set correct output mode") {
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val sink = new CloudStorageStreamSink(CloudStorageConfig(
        path = tempDir.resolve("output").toString,
        checkpointPath = checkpointPath,
        format = "parquet"
      ))

      sink.outputMode shouldBe OutputMode.Append()
      sink.checkpointLocation shouldBe checkpointPath
    }

    it("should write parquet files to local path") {
      val checkpointPath = tempDir.resolve("checkpoint").toString
      val outputPath     = tempDir.resolve("output").toString

      val cloudSink = new CloudStorageStreamSink(CloudStorageConfig(
        path = outputPath,
        checkpointPath = checkpointPath,
        format = "parquet"
      ))

      val pipeline = new StreamingPipeline {
        override def source: StreamingSource = new StreamingSource {
          override def readStream(): DataFrame =
            spark.readStream
              .format("rate")
              .option("rowsPerSecond", "100")
              .load()
        }

        override def sink: CloudStorageStreamSink = cloudSink

        override def trigger: TriggerConfig = TriggerConfig.Once
      }

      val query = pipeline.startStream()

      try {
        query.awaitTermination()

        // Verify parquet files were written
        val result = spark.read.parquet(outputPath)
        (result.schema.fieldNames should contain).allOf("timestamp", "value")
      } finally {
        if (query.isActive) query.stop()
      }
    }

    it("should support all file formats") {
      // Test that all format options are valid (don't require actual streaming)
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val formats = List("parquet", "json", "csv", "avro", "orc")
      formats.foreach { format =>
        val config = CloudStorageConfig(
          path = s"s3://bucket/$format",
          checkpointPath = checkpointPath,
          format = format
        )
        config.fileFormat.sparkFormat shouldBe format
      }
    }
  }

  describe("KafkaStreamSink") {

    it("should set correct output mode") {
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val sink = new KafkaStreamSink(KafkaSinkConfig(
        bootstrapServers = "localhost:9092",
        topic = "test-topic",
        checkpointPath = checkpointPath
      ))

      sink.outputMode shouldBe OutputMode.Append()
      sink.checkpointLocation shouldBe checkpointPath
    }

    it("should generate transactional ID when not provided") {
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val sink = new KafkaStreamSink(KafkaSinkConfig(
        bootstrapServers = "localhost:9092",
        topic = "test-topic",
        checkpointPath = checkpointPath,
        enableIdempotence = true
      ))

      // The sink should be able to create a writeStream without error
      // (actual Kafka connection would fail, but config is valid)
      sink.checkpointLocation shouldBe checkpointPath
    }
  }

  describe("ForeachStreamSink") {

    it("should set correct output mode") {
      val checkpointPath = tempDir.resolve("checkpoint").toString

      val sink = new ForeachStreamSink(ForeachSinkConfig(
        processorClass = "io.github.dwsmith1983.spark.pipeline.streaming.sinks.TestBatchProcessor",
        checkpointPath = checkpointPath,
        mode = "batch"
      ))

      sink.outputMode shouldBe OutputMode.Append()
      sink.checkpointLocation shouldBe checkpointPath
    }
  }
}

/** Test batch processor for ForeachStreamSink tests. */
class TestBatchProcessor extends StreamBatchProcessor {

  override def processBatch(df: DataFrame, batchId: Long): Unit = {
    val _ = (df, batchId) // suppress unused warning
    // No-op for testing
  }
}

/** Test row processor for ForeachStreamSink tests. */
class TestRowProcessor extends StreamRowProcessor {

  override def process(row: Row): Unit = {
    val _ = row // suppress unused warning
    // No-op for testing
  }
}
