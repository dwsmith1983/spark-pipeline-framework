package io.github.dwsmith1983.spark.pipeline.dq

import io.github.dwsmith1983.spark.pipeline.config.SparkConfig
import io.github.dwsmith1983.spark.pipeline.runtime.SparkSessionWrapper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach

import scala.jdk.CollectionConverters._

/** Tests for DataQualityCheck and built-in checks. */
class DataQualityCheckSpec
  extends AnyFunSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val config = SparkConfig(
      master = Some("local[2]"),
      appName = Some("DataQualityCheckTest"),
      config = Map(
        "spark.ui.enabled"             -> "false",
        "spark.sql.shuffle.partitions" -> "2"
      )
    )
    spark = SparkSessionWrapper.configure(config)
  }

  override def afterAll(): Unit =
    SparkSessionWrapper.stop()

  override def afterEach(): Unit = {
    spark.catalog.listTables().collect().foreach { t =>
      if (t.isTemporary) {
        spark.catalog.dropTempView(t.name)
      }
    }
  }

  // Helper to create DataFrames without spark.implicits
  private def createIntDf(name: String, data: Seq[Int]): Unit = {
    val schema = StructType(Seq(StructField("value", IntegerType, nullable = false)))
    val rows   = data.map(v => Row(v)).asJava
    spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
  }

  private def createTwoColDf(name: String, data: Seq[(String, Int)]): Unit = {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("value", IntegerType, nullable = false)
    ))
    val rows = data.map { case (n, v) => Row(n, v) }.asJava
    spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
  }

  private def createNullableTwoColDf(name: String, data: Seq[(Option[String], Int)]): Unit = {
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("value", IntegerType, nullable = false)
    ))
    val rows = data.map { case (n, v) => Row(n.orNull, v) }.asJava
    spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
  }

  describe("RowCountCheck") {

    it("should pass when row count meets minimum") {
      createIntDf("test_table", Seq(1, 2, 3, 4, 5))

      val check  = RowCountCheck("test_table", minRows = 5)
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Passed]
      result.asInstanceOf[DataQualityResult.Passed].details("actual_count") shouldBe 5L
    }

    it("should fail when row count below minimum") {
      createIntDf("small_table", Seq(1, 2, 3))

      val check  = RowCountCheck("small_table", minRows = 10)
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Failed]
      result.asInstanceOf[DataQualityResult.Failed].message should include("below minimum")
    }

    it("should fail when row count exceeds maximum") {
      createIntDf("large_table", Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

      val check  = RowCountCheck("large_table", minRows = 1, maxRows = Some(5))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Failed]
      result.asInstanceOf[DataQualityResult.Failed].message should include("exceeds maximum")
    }

    it("should skip when table not found") {
      val check  = RowCountCheck("nonexistent_table", minRows = 1)
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Skipped]
      result.asInstanceOf[DataQualityResult.Skipped].reason should include("not found")
    }

    it("should pass empty table with minRows = 0") {
      createIntDf("empty_table", Seq.empty)

      val check  = RowCountCheck("empty_table", minRows = 0)
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Passed]
    }

    it("should reject negative minRows") {
      an[IllegalArgumentException] should be thrownBy {
        RowCountCheck("table", minRows = -1)
      }
    }

    it("should reject maxRows less than minRows") {
      an[IllegalArgumentException] should be thrownBy {
        RowCountCheck("table", minRows = 10, maxRows = Some(5))
      }
    }
  }

  describe("NullCheck") {

    it("should pass when no nulls found") {
      createTwoColDf("no_nulls_table", Seq(("a", 1), ("b", 2), ("c", 3)))

      val check  = NullCheck("no_nulls_table", Seq("name", "value"), maxNullPercent = 0.0)
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Passed]
    }

    it("should fail when null percentage exceeds threshold") {
      createNullableTwoColDf("nulls_table", Seq(
        (Some("a"), 1),
        (None, 2),
        (Some("c"), 3),
        (None, 4),
        (Some("e"), 5)
      ))

      val check  = NullCheck("nulls_table", Seq("name"), maxNullPercent = 10.0)
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Failed]
      result.asInstanceOf[DataQualityResult.Failed].message should include("exceeds")
    }

    it("should pass when null percentage within threshold") {
      createNullableTwoColDf("some_nulls_table", Seq(
        (Some("a"), 1),
        (None, 2),
        (Some("c"), 3),
        (Some("d"), 4),
        (Some("e"), 5)
      ))

      val check  = NullCheck("some_nulls_table", Seq("name"), maxNullPercent = 25.0)
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Passed]
    }

    it("should skip when table not found") {
      val check  = NullCheck("missing_table", Seq("col"), maxNullPercent = 0.0)
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Skipped]
    }

    it("should pass on empty table") {
      createNullableTwoColDf("empty_null_table", Seq.empty)

      val check  = NullCheck("empty_null_table", Seq("name"), maxNullPercent = 0.0)
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Passed]
    }

    it("should reject empty columns list") {
      an[IllegalArgumentException] should be thrownBy {
        NullCheck("table", Seq.empty, maxNullPercent = 0.0)
      }
    }

    it("should reject invalid maxNullPercent") {
      an[IllegalArgumentException] should be thrownBy {
        NullCheck("table", Seq("col"), maxNullPercent = -1.0)
      }
      an[IllegalArgumentException] should be thrownBy {
        NullCheck("table", Seq("col"), maxNullPercent = 101.0)
      }
    }
  }

  describe("SchemaCheck") {

    it("should pass when all required columns present") {
      val schema = StructType(Seq(
        StructField("name", StringType),
        StructField("value", IntegerType),
        StructField("active", BooleanType)
      ))
      val rows = Seq(Row("a", 1, true)).asJava
      spark.createDataFrame(rows, schema).createOrReplaceTempView("schema_table")

      val check  = SchemaCheck("schema_table", Set("name", "value"))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Passed]
    }

    it("should fail when required columns missing") {
      createTwoColDf("incomplete_schema", Seq(("a", 1)))

      val check  = SchemaCheck("incomplete_schema", Set("name", "value", "timestamp"))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Failed]
      result.asInstanceOf[DataQualityResult.Failed].message should include("timestamp")
    }

    it("should skip when table not found") {
      val check  = SchemaCheck("no_table", Set("col"))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Skipped]
    }

    it("should reject empty required columns") {
      an[IllegalArgumentException] should be thrownBy {
        SchemaCheck("table", Set.empty)
      }
    }
  }

  describe("UniqueCheck") {

    it("should pass when all values unique") {
      createIntDf("unique_table", Seq(1, 2, 3, 4, 5))

      val check  = UniqueCheck("unique_table", Seq("value"))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Passed]
    }

    it("should fail when duplicates found") {
      createIntDf("dup_table", Seq(1, 2, 2, 3, 3, 3))

      val check  = UniqueCheck("dup_table", Seq("value"), maxDuplicatePercent = 0.0)
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Failed]
    }

    it("should pass when duplicates within threshold") {
      createIntDf("some_dup_table", Seq(1, 2, 2, 3, 4, 5))

      val check  = UniqueCheck("some_dup_table", Seq("value"), maxDuplicatePercent = 20.0)
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Passed]
    }

    it("should check composite keys") {
      val schema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("code", StringType)
      ))
      val rows = Seq(
        Row(1, "a"),
        Row(1, "b"),
        Row(2, "a"),
        Row(2, "b")
      ).asJava
      spark.createDataFrame(rows, schema).createOrReplaceTempView("composite_key_table")

      val check  = UniqueCheck("composite_key_table", Seq("id", "code"))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Passed]
    }

    it("should skip when table not found") {
      val check  = UniqueCheck("missing", Seq("col"))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Skipped]
    }

    it("should handle sampling") {
      createIntDf("large_unique_table", (1 to 1000).toSeq)

      val check  = UniqueCheck("large_unique_table", Seq("value"), sampleFraction = Some(0.5))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Passed]
    }
  }

  describe("RangeCheck") {

    it("should pass when all values in range") {
      createIntDf("range_table", Seq(10, 20, 30, 40, 50))

      val check  = RangeCheck("range_table", "value", min = Some(0), max = Some(100))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Passed]
    }

    it("should fail when values below minimum") {
      createIntDf("below_min_table", Seq(-5, 10, 20))

      val check  = RangeCheck("below_min_table", "value", min = Some(0))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Failed]
      result.asInstanceOf[DataQualityResult.Failed].message should include("outside range")
    }

    it("should fail when values above maximum") {
      createIntDf("above_max_table", Seq(50, 100, 200))

      val check  = RangeCheck("above_max_table", "value", max = Some(100))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Failed]
    }

    it("should skip when column not found") {
      createIntDf("wrong_col_table", Seq(1, 2, 3))

      val check  = RangeCheck("wrong_col_table", "other_col", min = Some(0))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Skipped]
      result.asInstanceOf[DataQualityResult.Skipped].reason should include("not found")
    }

    it("should skip when table not found") {
      val check  = RangeCheck("missing", "col", min = Some(0))
      val result = check.run(spark)

      result shouldBe a[DataQualityResult.Skipped]
    }

    it("should require at least one bound") {
      an[IllegalArgumentException] should be thrownBy {
        RangeCheck("table", "col", min = None, max = None)
      }
    }

    it("should reject max less than min") {
      an[IllegalArgumentException] should be thrownBy {
        RangeCheck("table", "col", min = Some(100), max = Some(10))
      }
    }
  }

  describe("CustomSqlCheck") {

    it("should pass when expectation met") {
      createIntDf("sql_table", Seq(1, 2, 3, 4, 5))

      val check = CustomSqlCheck(
        checkName = "avg_check",
        tableName = "sql_table",
        sql = "SELECT AVG(value) as avg_val FROM sql_table",
        expectation = row => row.getDouble(0) == 3.0
      )

      val result = check.run(spark)
      result shouldBe a[DataQualityResult.Passed]
    }

    it("should fail when expectation not met") {
      createIntDf("fail_sql_table", Seq(1, 2, 3))

      val check = CustomSqlCheck(
        checkName = "sum_check",
        tableName = "fail_sql_table",
        sql = "SELECT SUM(value) as total FROM fail_sql_table",
        expectation = row => row.getLong(0) > 100
      )

      val result = check.run(spark)
      result shouldBe a[DataQualityResult.Failed]
    }

    it("should fail gracefully on SQL error") {
      val check = CustomSqlCheck(
        checkName = "bad_sql",
        tableName = "table",
        sql = "SELECT * FROM nonexistent_xyz_table",
        expectation = _ => true
      )

      val result = check.run(spark)
      result shouldBe a[DataQualityResult.Failed]
      result.asInstanceOf[DataQualityResult.Failed].message should include("execution failed")
    }

    it("should use custom failure message") {
      createIntDf("custom_msg_table", Seq(1))

      val check = CustomSqlCheck(
        checkName = "custom",
        tableName = "custom_msg_table",
        sql = "SELECT COUNT(*) FROM custom_msg_table",
        expectation = row => row.getLong(0) > 100,
        failureMessage = Some("Expected more than 100 rows")
      )

      val result = check.run(spark)
      result shouldBe a[DataQualityResult.Failed]
      result.asInstanceOf[DataQualityResult.Failed].message shouldBe "Expected more than 100 rows"
    }
  }

  describe("CustomCheck") {

    it("should pass with custom validation logic") {
      createIntDf("custom_table", Seq(1, 2, 3, 4, 5))

      val check = CustomCheck(
        checkName = "custom_validation",
        tableName = "custom_table",
        validate = (_, df) => {
          val count = df.count()
          if (count >= 5) {
            DataQualityResult.Passed("custom_validation", "custom_table", Map("count" -> count))
          } else {
            DataQualityResult.Failed("custom_validation", "custom_table", "Too few rows")
          }
        }
      )

      val result = check.run(spark)
      result shouldBe a[DataQualityResult.Passed]
    }

    it("should fail with custom validation logic") {
      createIntDf("fail_custom_table", Seq(1, 2))

      val check = CustomCheck(
        checkName = "custom_fail",
        tableName = "fail_custom_table",
        validate = (_, df) => {
          val count = df.count()
          if (count >= 10) {
            DataQualityResult.Passed("custom_fail", "fail_custom_table")
          } else {
            DataQualityResult.Failed("custom_fail", "fail_custom_table", s"Only $count rows")
          }
        }
      )

      val result = check.run(spark)
      result shouldBe a[DataQualityResult.Failed]
    }

    it("should skip when table not found") {
      val check = CustomCheck(
        checkName = "missing_check",
        tableName = "missing_custom_table",
        validate = (_, _) => DataQualityResult.Passed("missing_check", "missing_custom_table")
      )

      val result = check.run(spark)
      result shouldBe a[DataQualityResult.Skipped]
    }

    it("should handle exceptions in validation") {
      createIntDf("exception_table", Seq(1))

      val check = CustomCheck(
        checkName = "exception_check",
        tableName = "exception_table",
        validate = (_, _) => throw new RuntimeException("Validation error")
      )

      val result = check.run(spark)
      result shouldBe a[DataQualityResult.Failed]
      result.asInstanceOf[DataQualityResult.Failed].message should include("exception")
    }
  }
}
