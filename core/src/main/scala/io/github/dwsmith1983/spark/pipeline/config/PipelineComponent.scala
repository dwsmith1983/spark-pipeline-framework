package io.github.dwsmith1983.spark.pipeline.config

/**
 * Base trait for all pipeline components.
 *
 * Each component represents a unit of work in a pipeline that can be
 * executed via the `run()` method. Components are instantiated by the
 * SimplePipelineRunner based on the pipeline configuration.
 */
trait PipelineComponent {

  /**
   * The name of this component instance.
   * Typically set from the `instance-name` in the config.
   */
  def name: String = this.getClass.getSimpleName

  /**
   * Executes the component's main logic.
   *
   * This method is called by the SimplePipelineRunner after the component
   * has been instantiated. All business logic should be implemented here.
   */
  def run(): Unit
}
