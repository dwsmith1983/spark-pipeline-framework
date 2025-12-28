package io.github.dwsmith1983.spark.pipeline.config

import org.scalacheck.{Arbitrary, Gen}

/**
 * ScalaCheck generators for property-based testing of config models.
 *
 * These generators produce valid configuration data that can be serialized
 * to HOCON and parsed back, enabling roundtrip property testing.
 *
 * @note We generate "model" representations rather than the actual case classes
 *       because `ComponentConfig.instanceConfig` is a typesafe `Config` object
 *       which is not easily constructed programmatically. Instead, we generate
 *       HOCON strings and verify parsing.
 */
object PropertyTestGenerators {

  // ============================================================================
  // Primitive Generators
  // ============================================================================

  /**
   * Generates valid identifiers (alphanumeric, starts with letter).
   * Used for class names, variable names, etc.
   */
  val genIdentifier: Gen[String] = for {
    first <- Gen.alphaChar
    rest  <- Gen.listOfN(Gen.choose(3, 20).sample.getOrElse(10), Gen.alphaNumChar)
  } yield (first :: rest).mkString

  /**
   * Generates valid pipeline/component names.
   * Allows spaces, hyphens, and common punctuation.
   */
  val genName: Gen[String] = for {
    words <- Gen.listOfN(Gen.choose(1, 4).sample.getOrElse(2), genIdentifier)
  } yield words.mkString(" ")

  /**
   * Generates fully qualified class names.
   * Format: com.example.package.ClassName
   */
  val genClassName: Gen[String] = for {
    packages  <- Gen.listOfN(Gen.choose(2, 4).sample.getOrElse(3), genIdentifier.map(_.toLowerCase))
    className <- genIdentifier.map(s => s"${s.head.toUpper}${s.tail}")
  } yield (packages :+ className).mkString(".")

  /** Generates Spark master URLs. */
  val genMaster: Gen[String] = Gen.oneOf(
    Gen.const("local[*]"),
    Gen.const("local[2]"),
    Gen.const("local[4]"),
    Gen.const("yarn"),
    Gen.const("yarn-client"),
    Gen.const("yarn-cluster"),
    Gen.choose(1, 10).map(n => s"local[$n]"),
    genIdentifier.map(h => s"spark://$h:7077")
  )

  // ============================================================================
  // Instance Config Generators (key-value pairs for component configs)
  // ============================================================================

  /**
   * Represents a key-value pair in instance-config.
   * Supports string, int, boolean, and nested configs.
   */
  sealed trait ConfigValue {
    def toHocon: String
  }

  case class StringValue(value: String) extends ConfigValue {
    def toHocon: String = s""""${escapeString(value)}""""
  }

  case class IntValue(value: Int) extends ConfigValue {
    def toHocon: String = value.toString
  }

  case class BoolValue(value: Boolean) extends ConfigValue {
    def toHocon: String = value.toString
  }

  case class ListValue(values: List[ConfigValue]) extends ConfigValue {
    def toHocon: String = values.map(_.toHocon).mkString("[", ", ", "]")
  }

  case class NestedValue(entries: Map[String, ConfigValue]) extends ConfigValue {

    def toHocon: String =
      if (entries.isEmpty) "{}"
      else entries.map { case (k, v) => s""""$k" = ${v.toHocon}""" }.mkString("{ ", ", ", " }")
  }

  private def escapeString(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")

  val genConfigValue: Gen[ConfigValue] = Gen.frequency(
    (5, Gen.alphaNumStr.map(StringValue)),
    (3, Gen.choose(-1000, 1000).map(IntValue)),
    (2, Gen.oneOf(true, false).map(BoolValue))
  )

  val genInstanceConfigEntries: Gen[Map[String, ConfigValue]] = for {
    size   <- Gen.choose(0, 5)
    keys   <- Gen.listOfN(size, Gen.alphaLowerStr.suchThat(_.nonEmpty).map(_.take(15)))
    values <- Gen.listOfN(size, genConfigValue)
  } yield keys.zip(values).toMap

  // ============================================================================
  // Component Model (for generation, not the actual case class)
  // ============================================================================

  /**
   * Model representation of ComponentConfig for property testing.
   * Can be converted to HOCON for parsing tests.
   */
  case class ComponentModel(
    instanceType: String,
    instanceName: String,
    instanceConfig: Map[String, ConfigValue]) {

    def toHocon: String =
      s"""{
         |  instance-type = "$instanceType"
         |  instance-name = "$instanceName"
         |  instance-config ${NestedValue(instanceConfig).toHocon}
         |}""".stripMargin
  }

  val genComponentModel: Gen[ComponentModel] = for {
    instanceType   <- genClassName
    instanceName   <- genName
    instanceConfig <- genInstanceConfigEntries
  } yield ComponentModel(instanceType, instanceName, instanceConfig)

  // ============================================================================
  // Pipeline Model (for generation, not the actual case class)
  // ============================================================================

  /**
   * Model representation of PipelineConfig for property testing.
   * Can be converted to HOCON for parsing tests.
   */
  case class PipelineModel(
    pipelineName: String,
    pipelineComponents: List[ComponentModel],
    failFast: Boolean) {

    def toHocon: String = {
      val componentsHocon = pipelineComponents.map(_.toHocon).mkString(",\n")
      s"""{
         |  pipeline-name = "$pipelineName"
         |  fail-fast = $failFast
         |  pipeline-components = [
         |    $componentsHocon
         |  ]
         |}""".stripMargin
    }
  }

  val genPipelineModel: Gen[PipelineModel] = for {
    pipelineName   <- genName
    componentCount <- Gen.choose(0, 5)
    components     <- Gen.listOfN(componentCount, genComponentModel)
    failFast       <- Gen.oneOf(true, false)
  } yield PipelineModel(pipelineName, components, failFast)

  /** Generator for non-empty pipeline (at least one component). */
  val genNonEmptyPipelineModel: Gen[PipelineModel] = for {
    pipelineName   <- genName
    componentCount <- Gen.choose(1, 5)
    components     <- Gen.listOfN(componentCount, genComponentModel)
    failFast       <- Gen.oneOf(true, false)
  } yield PipelineModel(pipelineName, components, failFast)

  // ============================================================================
  // SparkConfig Model
  // ============================================================================

  /** Model representation of SparkConfig for property testing. */
  case class SparkConfigModel(
    master: Option[String],
    appName: Option[String],
    config: Map[String, String]) {

    def toHocon: String = {
      val masterLine    = master.map(m => s"""master = "$m"""").getOrElse("")
      val appNameLine   = appName.map(n => s"""app-name = "$n"""").getOrElse("")
      val configEntries = config.map { case (k, v) => s""""$k" = "$v"""" }.mkString(", ")
      val configLine    = s"config { $configEntries }"

      Seq(masterLine, appNameLine, configLine).filter(_.nonEmpty).mkString("{\n  ", "\n  ", "\n}")
    }
  }

  val genSparkConfigModel: Gen[SparkConfigModel] = for {
    master     <- Gen.option(genMaster)
    appName    <- Gen.option(genName)
    configSize <- Gen.choose(0, 5)
    configKeys <- Gen.listOfN(
                    configSize,
                    Gen.oneOf(
                      "spark.executor.memory",
                      "spark.executor.cores",
                      "spark.driver.memory",
                      "spark.sql.shuffle.partitions",
                      "spark.dynamicAllocation.enabled"
                    )
                  )
    configValues <- Gen.listOfN(configSize, Gen.oneOf("2g", "4g", "2", "4", "100", "200", "true", "false"))
  } yield SparkConfigModel(master, appName, configKeys.zip(configValues).toMap)

  // ============================================================================
  // Arbitrary Instances (for ScalaTest integration)
  // ============================================================================

  implicit val arbPipelineModel: Arbitrary[PipelineModel]       = Arbitrary(genPipelineModel)
  implicit val arbComponentModel: Arbitrary[ComponentModel]     = Arbitrary(genComponentModel)
  implicit val arbSparkConfigModel: Arbitrary[SparkConfigModel] = Arbitrary(genSparkConfigModel)
}
