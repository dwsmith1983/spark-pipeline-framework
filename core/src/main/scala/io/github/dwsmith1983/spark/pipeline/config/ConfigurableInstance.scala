package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.Config

/**
 * Trait for companion objects that can create instances from HOCON config.
 *
 * User components should have a companion object extending this trait
 * to enable runtime instantiation by the SimplePipelineRunner.
 *
 * Example:
 * {{{
 * object MyPipeline extends ConfigurableInstance {
 *   case class MyConfig(inputTable: String, outputPath: String)
 *
 *   override def createFromConfig(conf: Config): MyPipeline = {
 *     import pureconfig._
 *     import pureconfig.generic.auto._
 *     new MyPipeline(ConfigSource.fromConfig(conf).loadOrThrow[MyConfig])
 *   }
 * }
 * }}}
 */
trait ConfigurableInstance {

  /**
   * Creates an instance of the component from the given config.
   *
   * @param conf The HOCON config containing instance-specific configuration
   * @return A new instance of the component
   */
  def createFromConfig(conf: Config): Any
}
