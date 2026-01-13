package io.github.dwsmith1983.spark.pipeline.streaming.config

/**
 * Controls how schema evolution is handled for streaming sources and sinks.
 *
 * Schema evolution allows the schema of streaming data to change over time
 * without breaking the pipeline. Different modes provide different levels
 * of flexibility.
 *
 * == Example ==
 *
 * {{{
 * val config = DeltaLakeConfig(
 *   path = "/data/events",
 *   schemaEvolutionMode = SchemaEvolutionMode.AddColumns
 * )
 * }}}
 *
 * @see [[DeltaLakeConfig]] for Delta Lake configuration
 * @see [[IcebergConfig]] for Apache Iceberg configuration
 */
sealed trait SchemaEvolutionMode

/** Companion object containing schema evolution mode implementations. */
object SchemaEvolutionMode {

  /**
   * Reject any schema changes.
   *
   * Use this mode when schema stability is critical and any changes
   * should cause the pipeline to fail.
   */
  case object None extends SchemaEvolutionMode

  /**
   * Allow adding new columns to the schema.
   *
   * This is a safe default that allows backward-compatible changes
   * while rejecting potentially breaking changes like column removal
   * or type changes.
   */
  case object AddColumns extends SchemaEvolutionMode

  /**
   * Allow all schema changes including column removal and type changes.
   *
   * Use with caution as this may lead to data quality issues if
   * downstream consumers are not prepared for schema changes.
   */
  case object FullEvolution extends SchemaEvolutionMode
}
