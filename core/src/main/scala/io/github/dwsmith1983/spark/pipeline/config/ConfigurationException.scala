package io.github.dwsmith1983.spark.pipeline.config

/**
 * Exception thrown when pipeline configuration is invalid.
 *
 * This exception wraps configuration parsing errors from the underlying
 * configuration library (PureConfig), providing a framework-specific
 * exception type that users can catch without depending on PureConfig internals.
 *
 * @param message A human-readable description of the configuration error
 * @param cause The underlying exception that caused this error (optional)
 */
class ConfigurationException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)

object ConfigurationException {

  /**
   * Creates a ConfigurationException from a PureConfig ConfigReaderException.
   *
   * Extracts the formatted error message from PureConfig's failures and
   * preserves the original exception as the cause for debugging purposes.
   *
   * @param e The PureConfig ConfigReaderException to wrap
   * @return A new ConfigurationException with a user-friendly message
   */
  def fromConfigReaderException(e: pureconfig.error.ConfigReaderException[_]): ConfigurationException =
    new ConfigurationException(e.failures.prettyPrint(), e)
}
