package io.github.dwsmith1983.spark.pipeline.dq

import io.github.dwsmith1983.spark.pipeline.config._
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * Configuration for a data quality check with timing specification.
 *
 * @param check The data quality check to execute
 * @param timing When to execute the check
 */
final case class DataQualityCheckConfig(check: DataQualityCheck, timing: CheckTiming)

/**
 * Pipeline lifecycle hooks that execute data quality checks at configured times.
 *
 * DataQualityHooks allows you to validate data at various points during
 * pipeline execution: before the pipeline starts, after specific components,
 * or after the entire pipeline completes.
 *
 * == Basic Usage ==
 *
 * {{{
 * val checks = Seq(
 *   DataQualityCheckConfig(
 *     RowCountCheck("raw_data", minRows = 1000),
 *     CheckTiming.AfterComponent("LoadRawData")
 *   ),
 *   DataQualityCheckConfig(
 *     NullCheck("cleaned_data", Seq("user_id"), maxNullPercent = 0.0),
 *     CheckTiming.AfterComponent("CleanData")
 *   ),
 *   DataQualityCheckConfig(
 *     SchemaCheck("output", Set("id", "name", "timestamp")),
 *     CheckTiming.AfterPipeline
 *   )
 * )
 *
 * val hooks = DataQualityHooks(spark, checks)
 * SimplePipelineRunner.run(config, hooks)
 * }}}
 *
 * == With Custom Failure Mode ==
 *
 * {{{
 * // Allow up to 2 check failures before stopping
 * val hooks = DataQualityHooks(
 *   spark = spark,
 *   checks = checks,
 *   failureMode = FailureMode.Threshold(2),
 *   sink = DataQualitySink.file("/var/log/dq-results.jsonl")
 * )
 * }}}
 *
 * == Combining with Other Hooks ==
 *
 * {{{
 * val combined = PipelineHooks.compose(
 *   DataQualityHooks(spark, checks),
 *   MetricsHooks(registry),
 *   LoggingHooks.structured()
 * )
 * SimplePipelineRunner.run(config, combined)
 * }}}
 *
 * @param spark The SparkSession used to execute checks
 * @param checks Sequence of checks with their timing configuration
 * @param failureMode How to handle check failures (default: FailOnError)
 * @param sink Where to write check results (default: logging sink)
 */
class DataQualityHooks(
  val spark: SparkSession,
  val checks: Seq[DataQualityCheckConfig],
  val failureMode: FailureMode = FailureMode.FailOnError,
  val sink: DataQualitySink = DataQualitySink.logging())
  extends PipelineHooks {

  private val logger: Logger                                 = LogManager.getLogger(classOf[DataQualityHooks])
  private val results: mutable.ListBuffer[DataQualityResult] = mutable.ListBuffer()
  private val failedChecks: mutable.ListBuffer[DataQualityResult.Failed] = mutable.ListBuffer()

  /**
   * Gets all check results collected during pipeline execution.
   * Results are available after checks have been run.
   */
  def getResults: Seq[DataQualityResult] = results.toSeq

  /** Gets all failed check results. */
  def getFailedResults: Seq[DataQualityResult.Failed] = failedChecks.toSeq

  /** Returns true if all checks passed (no failures). */
  def allChecksPassed: Boolean = failedChecks.isEmpty

  override def beforePipeline(config: PipelineConfig): Unit = {
    results.clear()
    failedChecks.clear()

    val beforeChecks = checks.filter(_.timing == CheckTiming.BeforePipeline)
    if (beforeChecks.nonEmpty) {
      logger.info("Running {} data quality checks before pipeline", beforeChecks.size: Any)
      runChecks(beforeChecks.map(_.check))
    }
  }

  override def afterComponent(
    config: ComponentConfig,
    index: Int,
    total: Int,
    durationMs: Long
  ): Unit = {
    locally { val _ = (index, total, durationMs) }

    val componentChecks = checks.filter {
      case DataQualityCheckConfig(_, CheckTiming.AfterComponent(name)) =>
        name == config.instanceName
      case _ => false
    }

    if (componentChecks.nonEmpty) {
      logger.info(
        "Running {} data quality checks after component '{}'",
        componentChecks.size: Any,
        config.instanceName: Any
      )
      runChecks(componentChecks.map(_.check))
    }
  }

  override def afterPipeline(config: PipelineConfig, result: PipelineResult): Unit = {
    locally { val _ = config }

    // Only run after-pipeline checks if pipeline didn't fail
    result match {
      case _: PipelineResult.Success | _: PipelineResult.PartialSuccess =>
        val afterChecks = checks.filter(_.timing == CheckTiming.AfterPipeline)
        if (afterChecks.nonEmpty) {
          logger.info("Running {} data quality checks after pipeline", afterChecks.size: Any)
          runChecks(afterChecks.map(_.check))
        }
      case _: PipelineResult.Failure =>
        logger.info("Skipping after-pipeline DQ checks due to pipeline failure")
    }

    // Write summary
    sink.flush()
    logSummary()
  }

  private def runChecks(checksToRun: Seq[DataQualityCheck]): Unit =
    checksToRun.foreach { check =>
      val result = executeCheck(check)
      results += result
      sink.write(result)

      result match {
        case f: DataQualityResult.Failed =>
          failedChecks += f
          handleFailure(f)
        case w: DataQualityResult.Warning =>
          logger.warn("Data quality warning: {} on {} - {}", check.name: Any, check.tableName: Any, w.message: Any)
        case s: DataQualityResult.Skipped =>
          logger.info("Data quality check skipped: {} on {} - {}", check.name: Any, check.tableName: Any, s.reason: Any)
        case p: DataQualityResult.Passed =>
          logger.debug("Data quality check passed: {} on {}", check.name: Any, check.tableName: Any)
          locally { val _ = p }
      }
    }

  private def executeCheck(check: DataQualityCheck): DataQualityResult =
    try {
      val startTime = System.currentTimeMillis()
      val result    = check.run(spark)
      val duration  = System.currentTimeMillis() - startTime
      logger.debug("Check '{}' completed in {}ms", check.name: Any, duration: Any)
      result
    } catch {
      case e: Exception =>
        logger.error("Check '{}' threw exception: {}", check.name: Any, e.getMessage: Any)
        DataQualityResult.Failed(
          check.name,
          check.tableName,
          s"Check threw exception: ${e.getMessage}",
          Map("exception_type" -> e.getClass.getSimpleName)
        )
    }

  private def handleFailure(failed: DataQualityResult.Failed): Unit =
    failureMode match {
      case FailureMode.FailOnError =>
        throw DataQualityException(failed.checkName, failed.tableName, failed.message)

      case FailureMode.WarnOnly =>
        logger.warn(
          "Data quality check failed (warn only): {} on {} - {}",
          failed.checkName: Any,
          failed.tableName: Any,
          failed.message: Any
        )

      case FailureMode.Threshold(maxFailures) =>
        if (failedChecks.size > maxFailures) {
          throw DataQualityException.multiple(failedChecks.toSeq)
        } else {
          logger.warn(
            "Data quality check failed ({}/{} threshold): {} on {} - {}",
            failedChecks.size: Any,
            maxFailures: Any,
            failed.checkName: Any,
            failed.tableName: Any,
            failed.message: Any
          )
        }
    }

  private def logSummary(): Unit = {
    val passed  = results.count(_.isPassed)
    val failed  = results.count(_.isFailed)
    val warned  = results.count(_.isWarning)
    val skipped = results.count(_.isSkipped)
    val total   = results.size

    if (failed > 0) {
      logger.warn(
        "Data quality summary: {}/{} passed, {} failed, {} warnings, {} skipped",
        passed: Any,
        total: Any,
        failed: Any,
        warned: Any,
        skipped: Any
      )
    } else {
      logger.info(
        "Data quality summary: {}/{} passed, {} warnings, {} skipped",
        passed: Any,
        total: Any,
        warned: Any,
        skipped: Any
      )
    }
  }
}

/** Factory methods for creating DataQualityHooks instances. */
object DataQualityHooks {

  /**
   * Creates DataQualityHooks with default settings.
   *
   * @param spark The SparkSession
   * @param checks The checks to execute
   * @return DataQualityHooks with FailOnError mode and logging sink
   */
  def apply(spark: SparkSession, checks: Seq[DataQualityCheckConfig]): DataQualityHooks =
    new DataQualityHooks(spark, checks)

  /**
   * Creates DataQualityHooks with custom failure mode.
   *
   * @param spark The SparkSession
   * @param checks The checks to execute
   * @param failureMode How to handle failures
   * @return Configured DataQualityHooks
   */
  def apply(
    spark: SparkSession,
    checks: Seq[DataQualityCheckConfig],
    failureMode: FailureMode
  ): DataQualityHooks =
    new DataQualityHooks(spark, checks, failureMode)

  /**
   * Creates DataQualityHooks with custom failure mode and sink.
   *
   * @param spark The SparkSession
   * @param checks The checks to execute
   * @param failureMode How to handle failures
   * @param sink Where to write results
   * @return Fully configured DataQualityHooks
   */
  def apply(
    spark: SparkSession,
    checks: Seq[DataQualityCheckConfig],
    failureMode: FailureMode,
    sink: DataQualitySink
  ): DataQualityHooks =
    new DataQualityHooks(spark, checks, failureMode, sink)

  /**
   * Creates DataQualityHooks with file output.
   *
   * @param spark The SparkSession
   * @param checks The checks to execute
   * @param outputPath Path to write JSON lines results
   * @return DataQualityHooks writing to file
   */
  def toFile(
    spark: SparkSession,
    checks: Seq[DataQualityCheckConfig],
    outputPath: String
  ): DataQualityHooks =
    new DataQualityHooks(spark, checks, sink = DataQualitySink.file(outputPath))

  /**
   * Creates DataQualityHooks in warn-only mode (no failures).
   *
   * @param spark The SparkSession
   * @param checks The checks to execute
   * @return DataQualityHooks that only warns on failures
   */
  def warnOnly(spark: SparkSession, checks: Seq[DataQualityCheckConfig]): DataQualityHooks =
    new DataQualityHooks(spark, checks, FailureMode.WarnOnly)
}
