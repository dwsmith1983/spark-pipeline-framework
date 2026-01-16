package io.github.dwsmith1983.pipelines.examples

import io.github.dwsmith1983.spark.pipeline.config._
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner
import com.typesafe.config.{Config, ConfigFactory}

import java.nio.file.{Files, Path}

/**
 * Canonical example demonstrating batch processing with spark-pipeline-framework.
 *
 * This end-to-end example showcases:
 * - Reading from CSV and Parquet sources
 * - Common batch transformations (filter, aggregate, join)
 * - Writing to multiple formats
 * - Configuration-driven multi-component pipelines
 * - Lifecycle hooks for monitoring and metrics
 * - Error handling and result reporting
 *
 * == Batch Processing Patterns ==
 *
 * This example demonstrates three fundamental batch patterns:
 *
 * 1. **ETL (Extract-Transform-Load)**: Read raw CSV data, clean and transform it, write to Parquet
 * 2. **Aggregation**: Compute summary statistics and group-by aggregations
 * 3. **Data Enrichment**: Join multiple datasets to create enriched output
 *
 * == Running ==
 *
 * {{{
 * # From sbt
 * sbt "examplespark3/runMain io.github.dwsmith1983.pipelines.BatchPipelineExample"
 *
 * # Or with spark-submit
 * spark-submit \
 *   --class io.github.dwsmith1983.pipelines.BatchPipelineExample \
 *   --master local[*] \
 *   spark-pipeline-example-spark3_2.13.jar
 * }}}
 *
 * == Output ==
 *
 * Creates three datasets in temporary directories:
 * - `cleaned-sales/` - Cleaned and filtered sales data in Parquet format
 * - `sales-summary/` - Aggregated sales by product in JSON format
 * - `enriched-sales/` - Sales data enriched with product details in Parquet format
 *
 * == Learning Path ==
 *
 * 1. Start with the sample data creation to understand the input schema
 * 2. Review the pipeline configuration to see component composition
 * 3. Examine each component implementation for transformation logic
 * 4. Study the hooks to understand execution monitoring
 * 5. Run the example and inspect the output datasets
 *
 * @see [[io.github.dwsmith1983.spark.pipeline.config.PipelineConfig]]
 * @see [[io.github.dwsmith1983.spark.pipeline.runtime.DataFlow]]
 * @see [[io.github.dwsmith1983.spark.pipeline.config.PipelineHooks]]
 */
object BatchPipelineExample {

  def main(args: Array[String]): Unit = {
    println("=" * 80)
    println("  BATCH PIPELINE FRAMEWORK - CANONICAL EXAMPLE")
    println("=" * 80)
    println()
    println("This example demonstrates common batch processing patterns:")
    println("  1. ETL - Extract, Transform, Load")
    println("  2. Aggregation - Computing summary statistics")
    println("  3. Data Enrichment - Joining datasets")
    println()

    // Create temporary directories for demo data
    val tempDir: Path          = Files.createTempDirectory("batch-pipeline-example")
    val salesCsvPath: Path     = tempDir.resolve("sales.csv")
    val productsCsvPath: Path  = tempDir.resolve("products.csv")
    val cleanedSalesPath: Path = tempDir.resolve("cleaned-sales")
    val summaryPath: Path      = tempDir.resolve("sales-summary")
    val enrichedPath: Path     = tempDir.resolve("enriched-sales")

    try {
      // Create sample data
      println("[SETUP] Creating sample sales and product data...")
      createSampleSalesData(salesCsvPath)
      createSampleProductData(productsCsvPath)
      println(s"[SETUP] Sales data: $salesCsvPath")
      println(s"[SETUP] Product data: $productsCsvPath")
      println()

      // Build configuration programmatically
      // In production, load from HOCON file: ConfigFactory.load("pipeline.conf")
      val config: Config = ConfigFactory.parseString(s"""
        spark {
          master = "local[*]"
          app-name = "Batch Pipeline Example"
          config {
            "spark.ui.enabled" = "false"
            "spark.sql.shuffle.partitions" = "4"
            "spark.sql.adaptive.enabled" = "true"
          }
        }

        pipeline {
          pipeline-name = "Batch Processing Pipeline"
          pipeline-components = [
            # Component 1: ETL - Clean and validate sales data
            {
              instance-type = "io.github.dwsmith1983.pipelines.BatchPipelineExample$$SalesETL"
              instance-name = "SalesETL(clean and filter)"
              instance-config {
                input-path = "${salesCsvPath.toString.replace("\\", "/")}"
                output-path = "${cleanedSalesPath.toString.replace("\\", "/")}"
                min-amount = 0.0
                output-format = "parquet"
              }
            },
            # Component 2: Aggregation - Compute sales summary
            {
              instance-type = "io.github.dwsmith1983.pipelines.BatchPipelineExample$$SalesAggregation"
              instance-name = "SalesAggregation(by product)"
              instance-config {
                input-path = "${cleanedSalesPath.toString.replace("\\", "/")}"
                output-path = "${summaryPath.toString.replace("\\", "/")}"
                group-by-columns = ["product_id", "product_name"]
                output-format = "json"
              }
            },
            # Component 3: Enrichment - Join sales with product details
            {
              instance-type = "io.github.dwsmith1983.pipelines.BatchPipelineExample$$SalesEnrichment"
              instance-name = "SalesEnrichment(with product details)"
              instance-config {
                sales-path = "${cleanedSalesPath.toString.replace("\\", "/")}"
                products-path = "${productsCsvPath.toString.replace("\\", "/")}"
                output-path = "${enrichedPath.toString.replace("\\", "/")}"
                output-format = "parquet"
              }
            }
          ]
        }
      """)

      // Create monitoring hooks
      println("[RUNNING] Building pipeline with logging hooks...")
      val loggingHooks = new BatchLoggingHooks()

      // Execute the pipeline
      println("[RUNNING] Executing batch pipeline...")
      println()
      SimplePipelineRunner.run(config, loggingHooks)

      // Display results
      println()
      println("=" * 80)
      println("  PIPELINE RESULTS")
      println("=" * 80)
      println()
      println("Output Datasets:")
      println(s"  1. Cleaned Sales:   $cleanedSalesPath")
      println(s"  2. Sales Summary:   $summaryPath")
      println(s"  3. Enriched Sales:  $enrichedPath")
      println()

      println("[SUCCESS] Batch pipeline completed successfully!")
      println()
      println("Next Steps:")
      println("  - Inspect the output directories to see the results")
      println("  - Modify the configuration to try different parameters")
      println("  - Add your own components by extending DataFlow")
      println("  - See the documentation for advanced features:")
      println("    * Checkpointing and retry logic")
      println("    * Schema contracts and validation")
      println("    * Secrets management")
      println()

    } catch {
      case e: Exception =>
        println()
        println("[FAILED] Pipeline execution failed!")
        println(s"[ERROR] ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)

    } finally {
      // Cleanup temporary files
      println("[CLEANUP] Removing temporary files...")
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach { (p: Path) =>
          val _: Boolean = Files.deleteIfExists(p)
        }
      println("[DONE]")
    }
  }

  /**
   * Creates sample sales data in CSV format.
   *
   * Schema: transaction_id, product_id, product_name, quantity, amount, transaction_date
   */
  private def createSampleSalesData(path: Path): Unit = {
    val salesData = """transaction_id,product_id,product_name,quantity,amount,transaction_date
      |T001,P001,Laptop,1,1299.99,2024-01-15
      |T002,P002,Mouse,2,49.98,2024-01-15
      |T003,P003,Keyboard,1,79.99,2024-01-16
      |T004,P001,Laptop,1,1299.99,2024-01-16
      |T005,P004,Monitor,1,399.99,2024-01-17
      |T006,P002,Mouse,3,74.97,2024-01-17
      |T007,P003,Keyboard,2,159.98,2024-01-18
      |T008,P005,Webcam,1,129.99,2024-01-18
      |T009,P001,Laptop,2,2599.98,2024-01-19
      |T010,P004,Monitor,1,399.99,2024-01-19
      |T011,P006,Headphones,1,199.99,2024-01-20
      |T012,P002,Mouse,1,24.99,2024-01-20
      |T013,INVALID,-1,-999.99,2024-01-20
      |T014,P003,Keyboard,5,399.95,2024-01-21
      |T015,P007,USB Cable,10,99.90,2024-01-21
      |""".stripMargin
    val _: Path = Files.writeString(path, salesData)
  }

  /**
   * Creates sample product catalog data in CSV format.
   *
   * Schema: product_id, category, brand, stock_quantity, supplier
   */
  private def createSampleProductData(path: Path): Unit = {
    val productData = """product_id,category,brand,stock_quantity,supplier
      |P001,Computer,TechCorp,50,Global Supplies Inc
      |P002,Accessory,OfficeMax,500,Office Depot
      |P003,Accessory,TechCorp,200,Global Supplies Inc
      |P004,Display,ViewSonic,100,Display World
      |P005,Accessory,WebTech,150,Tech Distributors
      |P006,Audio,SoundPro,75,Audio Warehouse
      |P007,Accessory,CableMax,1000,Cable Factory
      |""".stripMargin
    val _: Path = Files.writeString(path, productData)
  }

  /**
   * Custom logging hooks to show execution flow.
   *
   * Demonstrates how to implement PipelineHooks for monitoring and observability.
   */
  class BatchLoggingHooks extends PipelineHooks {

    override def beforePipeline(config: PipelineConfig): Unit = {
      println(s"┌─ PIPELINE START: ${config.pipelineName}")
      println(s"│  Components: ${config.pipelineComponents.size}")
      println("│")
    }

    override def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit = {
      println(s"├─ [${index + 1}/$total] ${config.instanceName}")
      println(s"│  Type: ${config.instanceType}")
    }

    override def afterComponent(
      config: ComponentConfig,
      index: Int,
      total: Int,
      durationMs: Long
    ): Unit = {
      println(s"│  ✓ Completed in ${durationMs}ms")
      println("│")
    }

    override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit =
      result match {
        case PipelineResult.Success(duration, count) =>
          println(s"└─ PIPELINE SUCCESS: $count components in ${duration}ms")

        case PipelineResult.Failure(error, failed, completed) =>
          println(s"└─ PIPELINE FAILED after $completed components")
          failed.foreach(c => println(s"   Failed: ${c.instanceName}"))
          println(s"   Error: ${error.getMessage}")

        case PipelineResult.PartialSuccess(duration, succeeded, failed, failures) =>
          println(s"└─ PIPELINE PARTIAL: $succeeded succeeded, $failed failed in ${duration}ms")
          failures.foreach {
            case (c, e) =>
              println(s"   Failed: ${c.instanceName} - ${e.getMessage}")
          }
      }

    override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {
      println(s"│  ✗ FAILED: ${config.instanceName}")
      println(s"│  Error: ${error.getMessage}")
    }
  }

  // ============================================================================
  // BATCH COMPONENT IMPLEMENTATIONS
  // ============================================================================

  /**
   * Component 1: Sales ETL - Extract, Transform, Load
   *
   * Demonstrates:
   * - Reading CSV with schema inference
   * - Data validation and filtering (remove invalid records)
   * - Column transformations
   * - Writing to Parquet for efficient storage
   */
  object SalesETL extends ConfigurableInstance {

    import pureconfig._
    import pureconfig.generic.auto._

    case class Config(
      inputPath: String,
      outputPath: String,
      minAmount: Double = 0.0,
      outputFormat: String = "parquet")

    override def createFromConfig(conf: com.typesafe.config.Config): SalesETL =
      new SalesETL(ConfigSource.fromConfig(conf).loadOrThrow[Config])
  }

  class SalesETL(conf: SalesETL.Config) extends io.github.dwsmith1983.spark.pipeline.runtime.DataFlow {
    import spark.implicits._

    override val name: String = "SalesETL"

    override def run(): Unit = {
      logger.info(s"Reading sales data from: ${conf.inputPath}")

      // Read CSV with header and schema inference
      val rawSales = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(conf.inputPath)

      logger.info(s"Raw sales records: ${rawSales.count()}")

      // Clean and validate data
      val cleanedSales = rawSales
        .filter($"product_id".isNotNull && $"product_id" =!= "INVALID")
        .filter($"amount" >= conf.minAmount)
        .filter($"quantity" > 0)
        .withColumnRenamed("amount", "sale_amount")
        .withColumnRenamed("quantity", "sale_quantity")

      val cleanedCount = cleanedSales.count()
      logger.info(s"Cleaned sales records: $cleanedCount")

      // Write to output
      logger.info(s"Writing cleaned data to: ${conf.outputPath}")
      cleanedSales.write
        .format(conf.outputFormat)
        .mode("overwrite")
        .save(conf.outputPath)

      logger.info("ETL complete")
    }
  }

  /**
   * Component 2: Sales Aggregation
   *
   * Demonstrates:
   * - Reading from Parquet (efficient columnar format)
   * - Group-by aggregations (sum, count, avg)
   * - Multiple aggregation functions
   * - Writing to JSON for human readability
   */
  object SalesAggregation extends ConfigurableInstance {

    import pureconfig._
    import pureconfig.generic.auto._

    case class Config(
      inputPath: String,
      outputPath: String,
      groupByColumns: List[String],
      outputFormat: String = "json")

    override def createFromConfig(conf: com.typesafe.config.Config): SalesAggregation =
      new SalesAggregation(ConfigSource.fromConfig(conf).loadOrThrow[Config])
  }

  class SalesAggregation(conf: SalesAggregation.Config) extends io.github.dwsmith1983.spark.pipeline.runtime.DataFlow {
    import org.apache.spark.sql.functions._

    override val name: String = "SalesAggregation"

    override def run(): Unit = {
      logger.info(s"Reading sales data from: ${conf.inputPath}")

      val sales = spark.read.parquet(conf.inputPath)

      logger.info(s"Aggregating by: ${conf.groupByColumns.mkString(", ")}")

      // Compute aggregations
      val summary = sales
        .groupBy(conf.groupByColumns.map(col): _*)
        .agg(
          sum("sale_amount").alias("total_revenue"),
          sum("sale_quantity").alias("total_units_sold"),
          count("transaction_id").alias("transaction_count"),
          avg("sale_amount").alias("avg_sale_amount"),
          min("transaction_date").alias("first_sale_date"),
          max("transaction_date").alias("last_sale_date")
        )
        .orderBy(desc("total_revenue"))

      logger.info(s"Writing summary to: ${conf.outputPath}")
      summary.write
        .format(conf.outputFormat)
        .mode("overwrite")
        .save(conf.outputPath)

      logger.info(s"Aggregation complete. Generated ${summary.count()} summary records")
    }
  }

  /**
   * Component 3: Sales Enrichment
   *
   * Demonstrates:
   * - Reading from multiple sources (Parquet + CSV)
   * - Inner join to combine datasets
   * - Column selection and renaming
   * - Creating enriched output datasets
   */
  object SalesEnrichment extends ConfigurableInstance {

    import pureconfig._
    import pureconfig.generic.auto._

    case class Config(
      salesPath: String,
      productsPath: String,
      outputPath: String,
      outputFormat: String = "parquet")

    override def createFromConfig(conf: com.typesafe.config.Config): SalesEnrichment =
      new SalesEnrichment(ConfigSource.fromConfig(conf).loadOrThrow[Config])
  }

  class SalesEnrichment(conf: SalesEnrichment.Config) extends io.github.dwsmith1983.spark.pipeline.runtime.DataFlow {
    override val name: String = "SalesEnrichment"

    override def run(): Unit = {
      logger.info(s"Reading sales from: ${conf.salesPath}")
      logger.info(s"Reading products from: ${conf.productsPath}")

      val sales = spark.read.parquet(conf.salesPath)

      val products = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(conf.productsPath)

      logger.info("Joining sales with product details")

      // Join sales with product catalog
      val enriched = sales
        .join(products, Seq("product_id"), "inner")
        .select(
          sales("transaction_id"),
          sales("product_id"),
          sales("product_name"),
          products("category"),
          products("brand"),
          sales("sale_quantity"),
          sales("sale_amount"),
          sales("transaction_date"),
          products("supplier")
        )

      logger.info(s"Writing enriched data to: ${conf.outputPath}")
      enriched.write
        .format(conf.outputFormat)
        .mode("overwrite")
        .save(conf.outputPath)

      logger.info(s"Enrichment complete. Created ${enriched.count()} enriched records")
    }
  }
}
