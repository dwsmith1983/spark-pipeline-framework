package io.github.dwsmith1983.spark.pipeline.config

import scala.util.Try

/**
 * Utility for instantiating pipeline components via reflection.
 *
 * This class enables the SimplePipelineRunner to dynamically load and
 * instantiate user-defined components based on configuration, without
 * requiring compile-time dependencies on user code.
 */
object ComponentInstantiator {

  /**
   * Instantiates a component from its configuration.
   *
   * The process:
   * 1. Load the class specified by `instanceType`
   * 2. Get its companion object (which must extend ConfigurableInstance)
   * 3. Call `createFromConfig` with the `instanceConfig`
   *
   * @param componentConfig The component configuration containing type and config
   * @tparam T The expected type of the component
   * @return The instantiated component
   * @note Throws `ComponentInstantiationException` if instantiation fails
   */
  def instantiate[T](componentConfig: ComponentConfig): T = {
    val className: String = componentConfig.instanceType
    try {
      verifyClassExists(className)
      val companion = loadCompanionObject(className)
      createComponent[T](companion, componentConfig, className)
    } catch {
      case e: ComponentInstantiationException => throw e
      case e: ConfigurationException          => throw e
      case e: pureconfig.error.ConfigReaderException[_] =>
        throw ConfigurationException.fromConfigReaderException(e)
      case e: NoSuchFieldException =>
        throw new ComponentInstantiationException(
          s"Could not find companion object for: $className. " +
            "Ensure it has a companion object extending ConfigurableInstance.",
          e
        )
      case e: Exception =>
        throw new ComponentInstantiationException(
          s"Failed to instantiate component: $className",
          e
        )
    }
  }

  private def verifyClassExists(className: String): Unit =
    try {
      val _ = Class.forName(className)
    } catch {
      case _: ClassNotFoundException =>
        throw new ComponentInstantiationException(
          s"Could not find class: $className. Ensure the class is on the classpath."
        )
    }

  private def loadCompanionObject(className: String): AnyRef = {
    val companionClass: Class[_] =
      try {
        Class.forName(className + "$")
      } catch {
        case _: ClassNotFoundException =>
          throw new ComponentInstantiationException(
            s"Could not find companion object for: $className. " +
              "Ensure it has a companion object extending ConfigurableInstance."
          )
      }
    companionClass.getField("MODULE$").get(null)
  }

  private def createComponent[T](
    companion: AnyRef,
    config: ComponentConfig,
    className: String
  ): T =
    companion match {
      case configurable: ConfigurableInstance =>
        configurable.createFromConfig(config.instanceConfig).asInstanceOf[T]
      case _ =>
        throw new ComponentInstantiationException(
          s"Companion object of $className does not extend ConfigurableInstance"
        )
    }

  /**
   * Attempts to instantiate a component, returning a Try.
   *
   * @param componentConfig The component configuration
   * @tparam T The expected type of the component
   * @return Success with the component, or Failure with the exception
   */
  def tryInstantiate[T](componentConfig: ComponentConfig): Try[T] =
    Try(instantiate[T](componentConfig))
}

/** Exception thrown when component instantiation fails. */
class ComponentInstantiationException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)
