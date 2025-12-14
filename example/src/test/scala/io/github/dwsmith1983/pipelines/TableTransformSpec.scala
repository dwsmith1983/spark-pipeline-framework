package io.github.dwsmith1983.pipelines

import io.github.dwsmith1983.spark.pipeline.config.SparkConfig
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{Files, Path}

/** Comprehensive tests for TableTransform component. */
class TableTransformSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: Path       = _

  override def beforeAll(): Unit = {
    val config: SparkConfig = SparkConfig(
      master = Some("local[2]"),
      appName = Some("TableTransformTest"),
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
    tempDir = Files.createTempDirectory("tabletransform-test")
    setupTestTables()
  }

  override def afterEach(): Unit = {
    // Clean up temp views
    spark.catalog.dropTempView("test_users")
    spark.catalog.dropTempView("test_products")
    spark.catalog.dropTempView("empty_table")

    // Clean up temp directory
    import scala.reflect.io.Directory
    new Directory(tempDir.toFile).deleteRecursively()
  }

  private def setupTestTables(): Unit = {
    // Users table - using SQL to avoid implicits issue with var spark
    spark.sql("""
      CREATE OR REPLACE TEMP VIEW test_users AS
      SELECT * FROM VALUES
        (1, 'Alice', 'alice@test.com', true, 25),
        (2, 'Bob', 'bob@test.com', false, 30),
        (3, 'Charlie', 'charlie@test.com', true, 35),
        (4, 'Diana', 'diana@test.com', true, 28)
      AS t(id, name, email, active, age)
    """)

    // Products table
    spark.sql("""
      CREATE OR REPLACE TEMP VIEW test_products AS
      SELECT * FROM VALUES
        (1, 'Widget', 10.99, 100),
        (2, 'Gadget', 25.50, 50),
        (3, 'Gizmo', 5.99, 200)
      AS t(id, name, price, quantity)
    """)

    // Empty table
    spark.sql("CREATE OR REPLACE TEMP VIEW empty_table AS SELECT 1 as id WHERE 1=0")
  }

  describe("TableTransform") {

    // =========================================================================
    // HAPPY PATH TESTS
    // =========================================================================

    describe("happy path") {

      it("should transform entire table without filters") {
        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = outputPath
        )

        val component: TableTransform = new TableTransform(config)
        component.run()

        val result: DataFrame = spark.read.parquet(outputPath)
        result.count() shouldBe 4
        (result.columns should contain).allOf("id", "name", "email", "active", "age")
      }

      it("should select specific columns") {
        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = outputPath,
          selectColumns = List("id", "name", "email")
        )

        val component: TableTransform = new TableTransform(config)
        component.run()

        val result: DataFrame = spark.read.parquet(outputPath)
        result.columns.toSet shouldBe Set("id", "name", "email")
        result.columns should not contain "active"
        result.columns should not contain "age"
      }

      it("should apply filter condition") {
        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = outputPath,
          filterCondition = Some("active = true")
        )

        val component: TableTransform = new TableTransform(config)
        component.run()

        val result: DataFrame = spark.read.parquet(outputPath)
        result.count() shouldBe 3 // Alice, Charlie, Diana
        result.filter("active = false").count() shouldBe 0
      }

      it("should combine column selection and filtering") {
        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = outputPath,
          selectColumns = List("name", "age"),
          filterCondition = Some("age >= 30")
        )

        val component: TableTransform = new TableTransform(config)
        component.run()

        val result: DataFrame = spark.read.parquet(outputPath)
        result.count() shouldBe 2 // Bob, Charlie
        result.columns.toSet shouldBe Set("name", "age")
      }

      it("should write in different formats") {
        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = outputPath,
          writeFormat = "json"
        )

        val component: TableTransform = new TableTransform(config)
        component.run()

        val result: DataFrame = spark.read.json(outputPath)
        result.count() shouldBe 4
      }

      it("should support append write mode") {
        val outputPath: String = tempDir.resolve("output").toString

        // First write
        val config1: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = outputPath,
          filterCondition = Some("id <= 2"),
          writeMode = "overwrite"
        )
        new TableTransform(config1).run()

        // Append write
        val config2: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = outputPath,
          filterCondition = Some("id > 2"),
          writeMode = "append"
        )
        new TableTransform(config2).run()

        val result: DataFrame = spark.read.parquet(outputPath)
        result.count() shouldBe 4 // All rows
      }

      it("should create component from config") {
        val hocon: Config = ConfigFactory.parseString("""
          input-table = "test_table"
          output-path = "/tmp/output"
          select-columns = ["id", "name"]
          filter-condition = "active = true"
          write-format = "orc"
          write-mode = "append"
        """)

        val component: TableTransform = TableTransform.createFromConfig(hocon).asInstanceOf[TableTransform]

        component should not be null
      }
    }

    // =========================================================================
    // UNHAPPY PATH TESTS
    // =========================================================================

    describe("unhappy path") {

      it("should fail when table does not exist") {
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "non_existent_table",
          outputPath = tempDir.resolve("output").toString
        )

        val component: TableTransform = new TableTransform(config)

        val exception: Exception = intercept[Exception] {
          component.run()
        }

        exception.getMessage should (include("non_existent_table").or(include("Table or view not found")))
      }

      it("should fail when selecting non-existent column") {
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = tempDir.resolve("output").toString,
          selectColumns = List("id", "nonexistent_column")
        )

        val component: TableTransform = new TableTransform(config)

        val exception: Exception = intercept[Exception] {
          component.run()
        }

        exception.getMessage should include("nonexistent_column")
      }

      it("should fail with invalid SQL filter") {
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = tempDir.resolve("output").toString,
          filterCondition = Some("INVALID SQL SYNTAX !!!")
        )

        val component: TableTransform = new TableTransform(config)

        intercept[Exception] {
          component.run()
        }
      }
    }

    // =========================================================================
    // EDGE CASE TESTS
    // =========================================================================

    describe("edge cases") {

      it("should handle empty result after filtering") {
        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = outputPath,
          filterCondition = Some("age > 1000") // No matches
        )

        val component: TableTransform = new TableTransform(config)
        component.run()

        val result: DataFrame = spark.read.parquet(outputPath)
        result.count() shouldBe 0
      }

      it("should handle empty input table") {
        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "empty_table",
          outputPath = outputPath
        )

        val component: TableTransform = new TableTransform(config)
        component.run()

        val result: DataFrame = spark.read.parquet(outputPath)
        result.count() shouldBe 0
      }

      it("should handle selecting single column") {
        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = outputPath,
          selectColumns = List("name")
        )

        val component: TableTransform = new TableTransform(config)
        component.run()

        val result: DataFrame = spark.read.parquet(outputPath)
        result.columns shouldBe Array("name")
      }

      it("should handle complex filter conditions") {
        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = outputPath,
          filterCondition = Some("(active = true AND age >= 25) OR name = 'Bob'")
        )

        val component: TableTransform = new TableTransform(config)
        component.run()

        val result: DataFrame = spark.read.parquet(outputPath)
        result.count() shouldBe 4 // All match this complex condition
      }

      it("should handle SQL functions in filter") {
        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = outputPath,
          filterCondition = Some("UPPER(name) LIKE 'A%'")
        )

        val component: TableTransform = new TableTransform(config)
        component.run()

        val result: DataFrame = spark.read.parquet(outputPath)
        result.count() shouldBe 1 // Only Alice
      }

      it("should handle database.table format") {
        // Create table in default database with explicit reference
        spark.sql("CREATE OR REPLACE TEMP VIEW default_test_table AS SELECT 1 as id, 'test' as value")

        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "default_test_table",
          outputPath = outputPath
        )

        val component: TableTransform = new TableTransform(config)

        noException should be thrownBy {
          component.run()
        }

        spark.catalog.dropTempView("default_test_table")
      }

      it("should provide meaningful component name") {
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_users",
          outputPath = "/tmp/output"
        )

        val component: TableTransform = new TableTransform(config)

        component.name shouldBe "TableTransform(test_users)"
      }

      it("should handle numeric table operations") {
        val outputPath: String = tempDir.resolve("output").toString
        val config: TableTransform.TransformConfig = TableTransform.TransformConfig(
          inputTable = "test_products",
          outputPath = outputPath,
          filterCondition = Some("price > 10.0 AND quantity >= 50")
        )

        val component: TableTransform = new TableTransform(config)
        component.run()

        val result: DataFrame = spark.read.parquet(outputPath)
        result.count() shouldBe 2 // Widget (10.99, 100) and Gadget (25.50, 50)
      }
    }
  }
}
