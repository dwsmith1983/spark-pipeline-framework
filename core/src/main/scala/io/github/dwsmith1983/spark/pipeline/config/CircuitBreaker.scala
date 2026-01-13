package io.github.dwsmith1983.spark.pipeline.config

/**
 * Circuit breaker states.
 *
 * The circuit breaker pattern prevents cascading failures by temporarily
 * stopping requests to a failing component.
 */
sealed trait CircuitState {
  def name: String
}

object CircuitState {

  /** Normal operation - requests flow through. */
  case object Closed extends CircuitState {
    override def name: String = "closed"
  }

  /** Failure threshold exceeded - requests are rejected immediately. */
  case object Open extends CircuitState {
    override def name: String = "open"
  }

  /** Testing recovery - limited requests allowed to test if component recovered. */
  case object HalfOpen extends CircuitState {
    override def name: String = "half_open"
  }
}

/**
 * Configuration for circuit breaker behavior.
 *
 * == Example HOCON ==
 *
 * {{{
 * circuit-breaker {
 *   failure-threshold = 5
 *   reset-timeout-ms = 60000
 *   half-open-success-threshold = 2
 * }
 * }}}
 *
 * @param failureThreshold Number of consecutive failures before opening the circuit.
 * @param resetTimeoutMs Time in milliseconds before attempting to close an open circuit.
 * @param halfOpenSuccessThreshold Number of consecutive successes in half-open state
 *                                  required to close the circuit.
 */
case class CircuitBreakerConfig(
  failureThreshold: Int = 5,
  resetTimeoutMs: Long = 60000L,
  halfOpenSuccessThreshold: Int = 2) {

  require(failureThreshold > 0, "failureThreshold must be positive")
  require(resetTimeoutMs > 0, "resetTimeoutMs must be positive")
  require(halfOpenSuccessThreshold > 0, "halfOpenSuccessThreshold must be positive")
}

object CircuitBreakerConfig {

  /** Default circuit breaker configuration. */
  val Default: CircuitBreakerConfig = CircuitBreakerConfig()

  /** Sensitive configuration that opens quickly but recovers slowly. */
  val Sensitive: CircuitBreakerConfig = CircuitBreakerConfig(
    failureThreshold = 3,
    resetTimeoutMs = 120000L,
    halfOpenSuccessThreshold = 3
  )

  /** Resilient configuration that tolerates more failures. */
  val Resilient: CircuitBreakerConfig = CircuitBreakerConfig(
    failureThreshold = 10,
    resetTimeoutMs = 30000L,
    halfOpenSuccessThreshold = 1
  )
}

/**
 * Exception thrown when the circuit breaker is open.
 *
 * This exception indicates that the component has experienced too many
 * recent failures and requests are being rejected to prevent cascading failures.
 *
 * @param componentName Name of the component whose circuit is open
 * @param state Current circuit state
 */
class CircuitBreakerOpenException(
  val componentName: String,
  val state: CircuitState)
  extends RuntimeException(
    s"Circuit breaker is ${state.name} for component '$componentName'. " +
      "Too many recent failures - requests are being rejected."
  )

/**
 * Circuit breaker implementation for protecting components from cascading failures.
 *
 * The circuit breaker tracks failures and temporarily prevents execution when
 * a failure threshold is exceeded. After a timeout, it allows limited requests
 * to test if the component has recovered.
 *
 * == State Machine ==
 *
 * {{{
 * CLOSED ---[failure threshold reached]---> OPEN
 * OPEN   ---[reset timeout elapsed]-------> HALF_OPEN
 * HALF_OPEN ---[success threshold]--------> CLOSED
 * HALF_OPEN ---[any failure]--------------> OPEN
 * }}}
 *
 * == Thread Safety ==
 *
 * This implementation is thread-safe using synchronized blocks.
 *
 * @param componentName Name of the component being protected
 * @param config Circuit breaker configuration
 * @param clock Function that returns current time in milliseconds (for testing)
 */
class CircuitBreaker(
  val componentName: String,
  val config: CircuitBreakerConfig,
  clock: () => Long = () => System.currentTimeMillis()) {

  @volatile private var state: CircuitState   = CircuitState.Closed
  @volatile private var failureCount: Int     = 0
  @volatile private var successCount: Int     = 0
  @volatile private var lastFailureTime: Long = 0L
  @volatile private var lastStateChange: Long = clock()

  /** Returns the current circuit state. */
  def currentState: CircuitState = synchronized(state)

  /** Returns the current failure count. */
  def currentFailureCount: Int = synchronized(failureCount)

  /** Returns the current success count (relevant in half-open state). */
  def currentSuccessCount: Int = synchronized(successCount)

  /** Returns the timestamp of the last state change. */
  def lastStateChangeTime: Long = synchronized(lastStateChange)

  /**
   * Checks if execution can proceed.
   *
   * @return true if execution is allowed, false if circuit is open
   */
  def canExecute(): Boolean = synchronized {
    state match {
      case CircuitState.Closed   => true
      case CircuitState.HalfOpen => true
      case CircuitState.Open =>
        if (clock() - lastFailureTime >= config.resetTimeoutMs) {
          transitionTo(CircuitState.HalfOpen)
          true
        } else {
          false
        }
    }
  }

  /**
   * Records a successful execution.
   *
   * In closed state, resets the failure counter.
   * In half-open state, increments success counter and may close the circuit.
   */
  def recordSuccess(): Unit = synchronized {
    state match {
      case CircuitState.Closed =>
        failureCount = 0
      case CircuitState.HalfOpen =>
        successCount += 1
        if (successCount >= config.halfOpenSuccessThreshold) {
          transitionTo(CircuitState.Closed)
        }
      case CircuitState.Open =>
        // Should not happen if canExecute is used properly
        ()
    }
  }

  /**
   * Records a failed execution.
   *
   * In closed state, increments failure counter and may open the circuit.
   * In half-open state, immediately opens the circuit.
   */
  def recordFailure(): Unit = synchronized {
    lastFailureTime = clock()
    state match {
      case CircuitState.Closed =>
        failureCount += 1
        if (failureCount >= config.failureThreshold) {
          transitionTo(CircuitState.Open)
        }
      case CircuitState.HalfOpen =>
        transitionTo(CircuitState.Open)
      case CircuitState.Open =>
        // Already open, just update failure time
        ()
    }
  }

  /**
   * Manually resets the circuit breaker to closed state.
   *
   * Use with caution - this bypasses the normal recovery process.
   */
  def reset(): Unit = synchronized {
    state = CircuitState.Closed
    failureCount = 0
    successCount = 0
    lastStateChange = clock()
  }

  private def transitionTo(newState: CircuitState): Unit = {
    val oldState: CircuitState = state
    if (oldState != newState) {
      state = newState
      lastStateChange = clock()
      newState match {
        case CircuitState.Closed =>
          failureCount = 0
          successCount = 0
        case CircuitState.HalfOpen =>
          successCount = 0
        case CircuitState.Open =>
          successCount = 0
      }
    }
  }
}

object CircuitBreaker {

  /**
   * Creates a circuit breaker with default configuration.
   *
   * @param componentName Name of the component being protected
   * @return A new CircuitBreaker instance
   */
  def apply(componentName: String): CircuitBreaker =
    new CircuitBreaker(componentName, CircuitBreakerConfig.Default)

  /**
   * Creates a circuit breaker with custom configuration.
   *
   * @param componentName Name of the component being protected
   * @param config Circuit breaker configuration
   * @return A new CircuitBreaker instance
   */
  def apply(componentName: String, config: CircuitBreakerConfig): CircuitBreaker =
    new CircuitBreaker(componentName, config)
}
