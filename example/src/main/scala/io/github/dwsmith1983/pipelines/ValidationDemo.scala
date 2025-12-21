package io.github.dwsmith1983.pipelines

import io.github.dwsmith1983.spark.pipeline.config._
import io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner
import com.typesafe.config.ConfigFactory

import java.nio.file.{Files, Path}

/**
 * Demonstrates dry-run validation and continue-on-error mode.
 *
 * This example shows:
 * - Validating pipeline configs without execution using `dryRun()`
 * - Running pipelines with `fail-fast = false` to continue after failures
 * - Handling `PartialSuccess` results
 *
 * == Running ==
 *
 * {{{
 * sbt "examplespark3/runMain io.github.dwsmith1983.pipelines.ValidationDemo"
 * }}}
 */
object ValidationDemo {

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("  VALIDATION & ERROR HANDLING DEMO")
    println("=" * 70)

    val tempDir: Path = Files.createTempDirectory("validation-demo")

    try {
      demoDryRun(tempDir)
      println()
      demoContinueOnError(tempDir)
    } finally {
      Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach { (p: Path) =>
          val _: Boolean = Files.deleteIfExists(p)
        }
    }
  }

  /** Demonstrates dry-run validation. */
  private def demoDryRun(tempDir: Path): Unit = {
    println("\n" + "-" * 70)
    println("  PART 1: DRY-RUN VALIDATION")
    println("-" * 70)

    val inputFile: Path = tempDir.resolve("input.txt")
    Files.writeString(inputFile, "hello world")

    // Valid configuration
    val validConfig = ConfigFactory.parseString(s"""
      spark {
        master = "local[*]"
        app-name = "DryRun Demo"
        config { "spark.ui.enabled" = "false" }
      }
      pipeline {
        pipeline-name = "Valid Pipeline"
        pipeline-components = [
          {
            instance-type = "io.github.dwsmith1983.pipelines.WordCount"
            instance-name = "WordCount"
            instance-config {
              input-path = "${inputFile.toString.replace("\\", "/")}"
              output-path = "${tempDir.resolve("output").toString.replace("\\", "/")}"
            }
          }
        ]
      }
    """)

    println("\n[TEST 1] Validating correct configuration...")
    SimplePipelineRunner.dryRun(validConfig) match {
      case DryRunResult.Valid(name, count, _) =>
        println(s"  ✓ Pipeline '$name' is valid with $count component(s)")
      case DryRunResult.Invalid(errors) =>
        println(s"  ✗ Unexpected validation failure: ${errors.size} error(s)")
    }

    // Invalid configuration - nonexistent component class
    val invalidConfig = ConfigFactory.parseString("""
      spark {
        master = "local[*]"
        app-name = "Invalid Demo"
        config { "spark.ui.enabled" = "false" }
      }
      pipeline {
        pipeline-name = "Invalid Pipeline"
        pipeline-components = [
          {
            instance-type = "com.nonexistent.FakeComponent"
            instance-name = "Ghost"
            instance-config {}
          },
          {
            instance-type = "also.does.not.Exist"
            instance-name = "AnotherGhost"
            instance-config {}
          }
        ]
      }
    """)

    println("\n[TEST 2] Validating configuration with invalid components...")
    SimplePipelineRunner.dryRun(invalidConfig) match {
      case DryRunResult.Valid(_, _, _) =>
        println("  ✗ Unexpected success")
      case DryRunResult.Invalid(errors) =>
        println(s"  ✓ Caught ${errors.size} validation error(s):")
        errors.foreach(error => println(s"    - ${error.message.take(60)}..."))
    }

    println("\n[RESULT] Dry-run validates configs without executing components")
  }

  /** Demonstrates continue-on-error mode with fail-fast = false. */
  private def demoContinueOnError(tempDir: Path): Unit = {
    println("\n" + "-" * 70)
    println("  PART 2: CONTINUE-ON-ERROR MODE")
    println("-" * 70)

    val inputFile: Path = tempDir.resolve("input2.txt")
    Files.writeString(inputFile, "testing continue on error mode")

    // Pipeline with fail-fast = false and a failing component
    val config = ConfigFactory.parseString(s"""
      spark {
        master = "local[*]"
        app-name = "ContinueOnError Demo"
        config { "spark.ui.enabled" = "false" }
      }
      pipeline {
        pipeline-name = "Resilient Pipeline"
        fail-fast = false
        pipeline-components = [
          {
            instance-type = "io.github.dwsmith1983.pipelines.WordCount"
            instance-name = "Step1-WordCount"
            instance-config {
              input-path = "${inputFile.toString.replace("\\", "/")}"
              output-path = "${tempDir.resolve("step1").toString.replace("\\", "/")}"
            }
          },
          {
            instance-type = "io.github.dwsmith1983.pipelines.FailingComponent"
            instance-name = "Step2-WillFail"
            instance-config {
              error-message = "Simulated failure for demo"
            }
          },
          {
            instance-type = "io.github.dwsmith1983.pipelines.WordCount"
            instance-name = "Step3-WordCount"
            instance-config {
              input-path = "${inputFile.toString.replace("\\", "/")}"
              output-path = "${tempDir.resolve("step3").toString.replace("\\", "/")}"
            }
          }
        ]
      }
    """)

    println("\n[RUNNING] Pipeline with fail-fast = false...")
    println("          (Step 2 will fail, but Step 3 should still run)")
    println()

    var finalResult: PipelineResult = null

    val hooks = new PipelineHooks {
      override def beforeComponent(config: ComponentConfig, index: Int, total: Int): Unit =
        println(s"  [${index + 1}/$total] Starting: ${config.instanceName}")

      override def afterComponent(
        config: ComponentConfig,
        index: Int,
        total: Int,
        durationMs: Long
      ): Unit =
        println(s"  [${index + 1}/$total] ✓ Completed: ${config.instanceName}")

      override def onComponentFailure(config: ComponentConfig, index: Int, error: Throwable): Unit =
        println(s"  [${index + 1}] ✗ Failed: ${config.instanceName} - ${error.getMessage}")

      override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit =
        finalResult = result
    }

    SimplePipelineRunner.run(config, hooks)

    println()
    finalResult match {
      case PipelineResult.Success(duration, count) =>
        println(s"[RESULT] Success: $count components in ${duration}ms")

      case PipelineResult.PartialSuccess(duration, succeeded, failed, failures) =>
        println(s"[RESULT] PartialSuccess in ${duration}ms:")
        println(s"         ✓ $succeeded component(s) succeeded")
        println(s"         ✗ $failed component(s) failed:")
        failures.foreach {
          case (comp, err) =>
            println(s"           - ${comp.instanceName}: ${err.getMessage}")
        }

      case PipelineResult.Failure(error, comp, completed) =>
        println(s"[RESULT] Failure after $completed components")
        comp.foreach(c => println(s"         Failed at: ${c.instanceName}"))
        println(s"         Error: ${error.getMessage}")
    }

    println("\n[NOTE] With fail-fast = true (default), Step 3 would NOT have run")
  }
}

/** A component that always fails - for testing error handling. */
case class FailingComponentConfig(errorMessage: String)

object FailingComponent extends ConfigurableInstance {

  import pureconfig._
  import pureconfig.generic.auto._

  override def createFromConfig(conf: com.typesafe.config.Config): FailingComponent = {
    val config = ConfigSource.fromConfig(conf).loadOrThrow[FailingComponentConfig]
    new FailingComponent(config)
  }
}

class FailingComponent(conf: FailingComponentConfig)
  extends io.github.dwsmith1983.spark.pipeline.runtime.DataFlow {

  override def run(): Unit =
    throw new RuntimeException(conf.errorMessage)
}
