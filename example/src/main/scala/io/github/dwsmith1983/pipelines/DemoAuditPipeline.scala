package io.github.dwsmith1983.pipelines

import io.github.dwsmith1983.spark.pipeline.audit._
import io.github.dwsmith1983.spark.pipeline.config._
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner
import com.typesafe.config.{Config, ConfigFactory}

import java.nio.file.{Files, Path}
import scala.io.Source

/**
 * End-to-end demonstration of AuditHooks for persistent execution audit trails.
 *
 * This runnable example shows:
 * - File-based JSON Lines audit trail
 * - Sensitive value filtering (passwords, API keys)
 * - Combining AuditHooks with LoggingHooks
 * - Reading and displaying audit events after execution
 *
 * == Running ==
 *
 * {{{
 * # From sbt
 * sbt "examplespark3/runMain io.github.dwsmith1983.pipelines.DemoAuditPipeline"
 *
 * # Or with spark-submit
 * spark-submit \
 *   --class io.github.dwsmith1983.pipelines.DemoAuditPipeline \
 *   --master local[*] \
 *   spark-pipeline-example-spark3_2.13.jar
 * }}}
 */
object DemoAuditPipeline {

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("  SPARK PIPELINE FRAMEWORK - AUDIT TRAIL DEMO")
    println("=" * 70)
    println()

    // Create temp directories for demo data
    val tempDir: Path   = Files.createTempDirectory("audit-demo")
    val inputFile: Path = tempDir.resolve("sample-text.txt")
    val outputDir: Path = tempDir.resolve("wordcount-output")
    val auditFile: Path = tempDir.resolve("pipeline-audit.jsonl")

    try {
      // Create sample input data
      println("[SETUP] Creating sample input data...")
      val sampleText: String =
        """Apache Spark provides fast analytics on big data.
          |Spark is used for data processing and machine learning.
          |The pipeline framework simplifies Spark application development.
          |""".stripMargin
      val _: Path = Files.writeString(inputFile, sampleText)
      println(s"[SETUP] Input file: $inputFile")
      println(s"[SETUP] Audit log: $auditFile")
      println()

      // Build pipeline configuration with a secret value (will be redacted in audit)
      val config: Config = ConfigFactory.parseString(s"""
        spark {
          master = "local[*]"
          app-name = "Audit Trail Demo"
          config {
            "spark.ui.enabled" = "false"
            "spark.sql.shuffle.partitions" = "2"
          }
        }

        pipeline {
          pipeline-name = "Word Count with Audit Trail"
          pipeline-components = [
            {
              instance-type = "io.github.dwsmith1983.pipelines.WordCount"
              instance-name = "WordCount(demo)"
              instance-config {
                input-path = "${inputFile.toString.replace("\\", "/")}"
                output-path = "${outputDir.toString.replace("\\", "/")}"
                min-count = 1
                write-format = "json"
                # This secret will be redacted in audit logs!
                api-key = "super-secret-key-12345"
              }
            }
          ]
        }
      """)

      // Create AuditHooks with file-based sink
      // Events are written as JSON Lines (one JSON object per line)
      println("[HOOKS] Setting up audit hooks...")
      val auditHooks = AuditHooks.toFile(
        auditFile.toString,
        runId = "demo-run-001" // Custom run ID for correlation
      )

      // Create simple logging hooks for console output
      val loggingHooks = LoggingHooks.humanReadable()

      // Compose audit + logging hooks
      val combinedHooks = PipelineHooks.compose(auditHooks, loggingHooks)
      println("[HOOKS] AuditHooks + LoggingHooks composed")
      println()

      // Run the pipeline
      println("[RUNNING] Executing pipeline...")
      println("-" * 70)
      SimplePipelineRunner.run(config, combinedHooks)
      println("-" * 70)
      println()

      // Close the audit sink to flush all events
      auditHooks.sink.close()

      // Display the audit log contents
      println("=" * 70)
      println("  AUDIT LOG CONTENTS")
      println("=" * 70)
      println(s"File: $auditFile")
      println()

      val auditLines = Source.fromFile(auditFile.toFile).getLines().toList
      println(s"Total events: ${auditLines.size}")
      println()

      auditLines.zipWithIndex.foreach {
        case (line, idx) =>
          println(s"Event ${idx + 1}:")
          // Pretty print the JSON (basic formatting)
          val formatted = formatJson(line)
          println(formatted)
          println()
      }

      // Highlight security filtering
      println("=" * 70)
      println("  SECURITY: SENSITIVE VALUE FILTERING")
      println("=" * 70)
      println()
      println("Notice in the audit log above:")
      println("  - 'api-key' is REDACTED (***REDACTED***)")
      println("  - Only safe environment variables are included")
      println("  - Sensitive Spark properties are filtered")
      println()
      println("Default sensitive patterns: password, secret, key, token, credential, auth")
      println()

      // Show how to customize filtering
      println("=" * 70)
      println("  CUSTOMIZATION EXAMPLE")
      println("=" * 70)
      println()
      println("""
        |// Add custom sensitive patterns
        |val customFilter = ConfigFilter.withAdditionalPatterns(Set("internal", "private_data"))
        |
        |// Add custom allowed environment variables
        |val customEnvFilter = EnvFilter.withAdditionalAllowedKeys(Set("MY_APP_ENV", "DEPLOYMENT"))
        |
        |// Create AuditHooks with custom filters
        |val auditHooks = new AuditHooks(
        |  sink = AuditSink.file("/var/log/pipeline-audit.jsonl"),
        |  configFilter = customFilter,
        |  envFilter = customEnvFilter
        |)
        |""".stripMargin)

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

  /**
   * Basic JSON formatting for readability.
   * In production, use a proper JSON library.
   */
  private def formatJson(json: String): String = {
    var indent   = 0
    val result   = new StringBuilder
    var inString = false
    var escape   = false

    for (c <- json) {
      if (escape) {
        result.append(c)
        escape = false
      } else if (c == '\\') {
        result.append(c)
        escape = true
      } else if (c == '"') {
        inString = !inString
        result.append(c)
      } else if (!inString) {
        c match {
          case '{' | '[' =>
            result.append(c)
            indent += 2
            result.append('\n')
            result.append(" " * indent)
          case '}' | ']' =>
            indent -= 2
            result.append('\n')
            result.append(" " * indent)
            result.append(c)
          case ',' =>
            result.append(c)
            result.append('\n')
            result.append(" " * indent)
          case ':' =>
            result.append(": ")
          case _ =>
            result.append(c)
        }
      } else {
        result.append(c)
      }
    }
    result.toString()
  }
}
