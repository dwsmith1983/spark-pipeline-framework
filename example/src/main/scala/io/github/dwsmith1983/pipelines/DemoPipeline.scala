package io.github.dwsmith1983.pipelines

import io.github.dwsmith1983.spark.pipeline.config._
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner
import com.typesafe.config.{Config, ConfigFactory}

import java.nio.file.{Files, Path}

/**
 * End-to-end demonstration of spark-pipeline-framework.
 *
 * This runnable example shows:
 * - Programmatic config creation (can also use HOCON files)
 * - Multi-component pipeline execution
 * - MetricsHooks for execution monitoring
 * - PipelineResult handling for success/failure
 *
 * == Running ==
 *
 * {{{
 * # From sbt
 * sbt "examplespark3/runMain io.github.dwsmith1983.pipelines.DemoPipeline"
 *
 * # Or with spark-submit
 * spark-submit \
 *   --class io.github.dwsmith1983.pipelines.DemoPipeline \
 *   --master local[*] \
 *   spark-pipeline-example-spark3_2.13.jar
 * }}}
 */
object DemoPipeline {

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("  SPARK PIPELINE FRAMEWORK - END-TO-END DEMO")
    println("=" * 70)
    println()

    // Create temp directories for demo data
    val tempDir: Path    = Files.createTempDirectory("pipeline-demo")
    val inputFile: Path  = tempDir.resolve("sample-text.txt")
    val outputDir1: Path = tempDir.resolve("wordcount-output")
    val outputDir2: Path = tempDir.resolve("filtered-output")

    try {
      // Create sample input data
      println("[SETUP] Creating sample input data...")
      val sampleText: String =
        """Apache Spark is a unified analytics engine for large-scale data processing.
          |Spark provides high-level APIs in Java, Scala, Python, and R.
          |Spark also supports a rich set of higher-level tools including Spark SQL.
          |The pipeline framework makes it easy to build configuration-driven Spark applications.
          |Configuration-driven pipelines reduce boilerplate and improve maintainability.
          |Hooks provide visibility into pipeline execution with minimal code changes.
          |""".stripMargin
      val _: Path = Files.writeString(inputFile, sampleText)
      println(s"[SETUP] Input file: $inputFile")
      println()

      // Build pipeline configuration programmatically
      // (In production, this would typically come from a HOCON file)
      val config: Config = ConfigFactory.parseString(s"""
        spark {
          master = "local[*]"
          app-name = "Pipeline Demo"
          config {
            "spark.ui.enabled" = "false"
            "spark.sql.shuffle.partitions" = "4"
          }
        }

        pipeline {
          pipeline-name = "Word Analysis Pipeline"
          pipeline-components = [
            {
              instance-type = "io.github.dwsmith1983.pipelines.WordCount"
              instance-name = "WordCount(all words)"
              instance-config {
                input-path = "${inputFile.toString.replace("\\", "/")}"
                output-path = "${outputDir1.toString.replace("\\", "/")}"
                min-count = 1
                write-format = "parquet"
              }
            },
            {
              instance-type = "io.github.dwsmith1983.pipelines.WordCount"
              instance-name = "WordCount(frequent words only)"
              instance-config {
                input-path = "${inputFile.toString.replace("\\", "/")}"
                output-path = "${outputDir2.toString.replace("\\", "/")}"
                min-count = 2
                write-format = "json"
              }
            }
          ]
        }
      """)

      // Create metrics hooks to monitor execution
      val metrics = new MetricsHooks()

      // Custom logging hooks to show execution flow
      val loggingHooks = new PipelineHooks {
        override def beforePipeline(config: PipelineConfig): Unit =
          println(s"\n[PIPELINE START] ${config.pipelineName} (${config.pipelineComponents.size} components)")

        override def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit =
          println(s"  [${index + 1}/$total] Starting: ${config.instanceName}")

        override def afterComponent(
          config: ComponentConfig,
          index: Int,
          total: Int,
          durationMs: Long
        ): Unit =
          println(s"  [${index + 1}/$total] Completed: ${config.instanceName} (${durationMs}ms)")

        override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
          val _ = config
          result match {
            case PipelineResult.Success(duration, count) =>
              println(s"\n[PIPELINE COMPLETE] $count components executed in ${duration}ms")
            case PipelineResult.Failure(error, failed, completed) =>
              println(s"\n[PIPELINE FAILED] After $completed components")
              failed.foreach(c => println(s"  Failed component: ${c.instanceName}"))
              println(s"  Error: ${error.getMessage}")
          }
        }

        override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit = {
          val _ = error
          println(s"  [${index + 1}] FAILED: ${config.instanceName}")
        }
      }

      // Compose multiple hooks together
      val combinedHooks: PipelineHooks = PipelineHooks.compose(loggingHooks, metrics)

      // Run the pipeline
      println("[RUNNING] Executing pipeline with composed hooks...")
      SimplePipelineRunner.run(config, combinedHooks)

      // Display metrics report
      println()
      println("=" * 70)
      println("  METRICS REPORT")
      println("=" * 70)
      println(metrics.generateReport())

      // Show output locations
      println("=" * 70)
      println("  OUTPUT LOCATIONS")
      println("=" * 70)
      println(s"  All words:      $outputDir1")
      println(s"  Frequent words: $outputDir2")
      println()

    } finally {
      // Cleanup
      println("[CLEANUP] Removing temporary files...")
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach { (p: Path) =>
          val _: Boolean = Files.deleteIfExists(p)
        }
      println("[DONE]")
    }
  }
}
