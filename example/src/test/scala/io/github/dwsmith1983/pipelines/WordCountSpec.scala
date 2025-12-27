package io.github.dwsmith1983.pipelines

import io.github.dwsmith1983.spark.pipeline.config.SparkConfig
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{Files, Path}



/** Comprehensive tests for WordCount component. */
class WordCountSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: Path       = _

  override def beforeAll(): Unit = {
    val config: SparkConfig = SparkConfig(
      master = Some("local[2]"),
      appName = Some("WordCountTest"),
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
    tempDir = Files.createTempDirectory("wordcount-test")

  override def afterEach(): Unit = {
    // Clean up temp directory
    import scala.reflect.io.Directory
    new Directory(tempDir.toFile).deleteRecursively()
  }

  describe("WordCount") {

    // =========================================================================
    // HAPPY PATH TESTS
    // =========================================================================

    describe("happy path") {

      it("should count words in a single file") {
        val inputPath: Path = tempDir.resolve("input")
        Files.createDirectory(inputPath)
        Files.writeString(inputPath.resolve("file1.txt"), "hello world hello spark hello")

        val outputPath = tempDir.resolve("output").toString
        val config = WordCount.WordCountConfig(
          inputPath = inputPath.toString,
          outputPath = outputPath,
          minCount = 1,
          writeFormat = "parquet"
        )

        val component = new WordCount(config)
        component.run()

        val result = spark.read.parquet(outputPath)
        val counts = result.collect().map(r => (r.getString(0), r.getLong(1))).toMap

        counts("hello") shouldBe 3
        counts("world") shouldBe 1
        counts("spark") shouldBe 1
      }

      it("should count words across multiple files") {
        val inputPath: Path = tempDir.resolve("input")
        Files.createDirectory(inputPath)
        Files.writeString(inputPath.resolve("file1.txt"), "apple banana apple")
        Files.writeString(inputPath.resolve("file2.txt"), "banana cherry banana")

        val outputPath = tempDir.resolve("output").toString
        val config = WordCount.WordCountConfig(
          inputPath = inputPath.toString,
          outputPath = outputPath,
          minCount = 1
        )

        val component = new WordCount(config)
        component.run()

        val result = spark.read.parquet(outputPath)
        val counts = result.collect().map(r => (r.getString(0), r.getLong(1))).toMap

        counts("apple") shouldBe 2
        counts("banana") shouldBe 3
        counts("cherry") shouldBe 1
      }

      it("should filter by minCount threshold") {
        val inputPath = tempDir.resolve("input")
        Files.createDirectory(inputPath)
        Files.writeString(inputPath.resolve("file.txt"), "common common common rare rare single")

        val outputPath = tempDir.resolve("output").toString
        val config = WordCount.WordCountConfig(
          inputPath = inputPath.toString,
          outputPath = outputPath,
          minCount = 2 // Only words with count >= 2
        )

        val component = new WordCount(config)
        component.run()

        val result = spark.read.parquet(outputPath)
        val words  = result.collect().map(_.getString(0)).toSet

        words should contain("common")
        words should contain("rare")
        words should not contain "single" // count = 1, filtered out
      }

      it("should write in specified format") {
        val inputPath = tempDir.resolve("input")
        Files.createDirectory(inputPath)
        Files.writeString(inputPath.resolve("file.txt"), "test data")

        val outputPath = tempDir.resolve("output").toString
        val config = WordCount.WordCountConfig(
          inputPath = inputPath.toString,
          outputPath = outputPath,
          writeFormat = "json"
        )

        val component = new WordCount(config)
        component.run()

        // Should be able to read as JSON
        noException should be thrownBy {
          spark.read.json(outputPath)
        }
      }

      it("should create component from config") {
        val hocon = ConfigFactory.parseString("""
          input-path = "/tmp/input"
          output-path = "/tmp/output"
          min-count = 5
          write-format = "orc"
        """)

        val component = WordCount.createFromConfig(hocon).asInstanceOf[WordCount]

        component should not be null
      }
    }

    // =========================================================================
    // UNHAPPY PATH TESTS
    // =========================================================================

    describe("unhappy path") {

      it("should fail when input path does not exist") {
        val config = WordCount.WordCountConfig(
          inputPath = "/non/existent/path",
          outputPath = tempDir.resolve("output").toString
        )

        val component = new WordCount(config)

        val exception = intercept[Exception] {
          component.run()
        }

        // Spark throws AnalysisException for missing paths
        exception.getClass.getSimpleName should (
          equal("AnalysisException").or(equal("PathNotFoundException"))
        )
        exception.getMessage should include("/non/existent/path")
      }
    }

    // =========================================================================
    // EDGE CASE TESTS
    // =========================================================================

    describe("edge cases") {

      it("should handle empty input file") {
        val inputPath = tempDir.resolve("input")
        Files.createDirectory(inputPath)
        Files.writeString(inputPath.resolve("empty.txt"), "")

        val outputPath = tempDir.resolve("output").toString
        val config = WordCount.WordCountConfig(
          inputPath = inputPath.toString,
          outputPath = outputPath,
          minCount = 1
        )

        val component = new WordCount(config)
        component.run()

        val result = spark.read.parquet(outputPath)
        result.count() shouldBe 0
      }

      it("should handle file with only whitespace") {
        val inputPath = tempDir.resolve("input")
        Files.createDirectory(inputPath)
        Files.writeString(inputPath.resolve("whitespace.txt"), "   \n\t\n   ")

        val outputPath = tempDir.resolve("output").toString
        val config = WordCount.WordCountConfig(
          inputPath = inputPath.toString,
          outputPath = outputPath,
          minCount = 1
        )

        val component = new WordCount(config)
        component.run()

        val result = spark.read.parquet(outputPath)
        result.count() shouldBe 0
      }

      it("should handle special characters in words") {
        val inputPath = tempDir.resolve("input")
        Files.createDirectory(inputPath)
        Files.writeString(inputPath.resolve("special.txt"), "hello! world? hello, spark.")

        val outputPath = tempDir.resolve("output").toString
        val config = WordCount.WordCountConfig(
          inputPath = inputPath.toString,
          outputPath = outputPath,
          minCount = 1
        )

        val component = new WordCount(config)
        component.run()

        val result = spark.read.parquet(outputPath)
        // Words include punctuation since we split on whitespace
        result.count() should be > 0L
      }

      it("should handle unicode characters") {
        val inputPath = tempDir.resolve("input")
        Files.createDirectory(inputPath)
        Files.writeString(inputPath.resolve("unicode.txt"), "日本語 テスト 日本語 emoji 🚀 emoji")

        val outputPath = tempDir.resolve("output").toString
        val config = WordCount.WordCountConfig(
          inputPath = inputPath.toString,
          outputPath = outputPath,
          minCount = 1
        )

        val component = new WordCount(config)
        component.run()

        val result = spark.read.parquet(outputPath)
        val words  = result.collect().map(_.getString(0)).toSet

        words should contain("日本語")
        words should contain("emoji")
      }

      it("should handle very large minCount filtering all words") {
        val inputPath = tempDir.resolve("input")
        Files.createDirectory(inputPath)
        Files.writeString(inputPath.resolve("file.txt"), "one two three")

        val outputPath = tempDir.resolve("output").toString
        val config = WordCount.WordCountConfig(
          inputPath = inputPath.toString,
          outputPath = outputPath,
          minCount = 1000000 // Very high threshold
        )

        val component = new WordCount(config)
        component.run()

        val result = spark.read.parquet(outputPath)
        result.count() shouldBe 0
      }

      it("should handle single very long word") {
        val inputPath = tempDir.resolve("input")
        Files.createDirectory(inputPath)
        val longWord = "a" * 10000
        Files.writeString(inputPath.resolve("long.txt"), s"$longWord $longWord short")

        val outputPath = tempDir.resolve("output").toString
        val config = WordCount.WordCountConfig(
          inputPath = inputPath.toString,
          outputPath = outputPath,
          minCount = 1
        )

        val component = new WordCount(config)
        component.run()

        val result = spark.read.parquet(outputPath)
        val counts = result.collect().map(r => (r.getString(0), r.getLong(1))).toMap

        counts(longWord) shouldBe 2
        counts("short") shouldBe 1
      }
    }
  }
}
