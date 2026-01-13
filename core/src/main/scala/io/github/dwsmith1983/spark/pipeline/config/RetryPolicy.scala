package io.github.dwsmith1983.spark.pipeline.config

import scala.util.Random

/**
 * Configuration for retry behavior on component failures.
 *
 * RetryPolicy defines how the pipeline runner should handle transient failures
 * by specifying retry counts, backoff delays, and which exceptions are retryable.
 *
 * == Backoff Calculation ==
 *
 * The delay before retry attempt `n` (0-indexed) is calculated as:
 * {{{
 * baseDelay = min(initialDelayMs * (backoffMultiplier ^ n), maxDelayMs)
 * jitter = baseDelay * jitterFactor * random(-1, 1)
 * delay = max(0, baseDelay + jitter)
 * }}}
 *
 * == Example HOCON ==
 *
 * {{{
 * retry-policy {
 *   max-retries = 3
 *   initial-delay-ms = 1000
 *   max-delay-ms = 30000
 *   backoff-multiplier = 2.0
 *   jitter-factor = 0.1
 * }
 * }}}
 *
 * @param maxRetries Maximum number of retry attempts. 0 means no retries.
 * @param initialDelayMs Delay in milliseconds before the first retry.
 * @param maxDelayMs Maximum delay cap for exponential backoff.
 * @param backoffMultiplier Multiplier for exponential backoff (e.g., 2.0 doubles delay each retry).
 * @param jitterFactor Random jitter factor (0.1 = ±10% of base delay). Prevents thundering herd.
 * @param retryableExceptions Set of exception class names that are retryable.
 *                            Empty set means all exceptions are retryable.
 */
case class RetryPolicy(
  maxRetries: Int = 3,
  initialDelayMs: Long = 1000L,
  maxDelayMs: Long = 30000L,
  backoffMultiplier: Double = 2.0,
  jitterFactor: Double = 0.1,
  retryableExceptions: Set[String] = Set.empty) {

  require(maxRetries >= 0, "maxRetries must be non-negative")
  require(initialDelayMs >= 0, "initialDelayMs must be non-negative")
  require(maxDelayMs >= 0, "maxDelayMs must be non-negative")
  require(backoffMultiplier >= 1.0, "backoffMultiplier must be >= 1.0")
  require(jitterFactor >= 0.0 && jitterFactor <= 1.0, "jitterFactor must be between 0.0 and 1.0")
}

object RetryPolicy {

  /** No retries - fail immediately on first error. */
  val NoRetry: RetryPolicy = RetryPolicy(maxRetries = 0)

  /** Default retry policy with sensible defaults. */
  val Default: RetryPolicy = RetryPolicy()

  /** Aggressive retry policy for highly transient failures. */
  val Aggressive: RetryPolicy = RetryPolicy(
    maxRetries = 5,
    initialDelayMs = 500L,
    maxDelayMs = 60000L,
    backoffMultiplier = 2.0,
    jitterFactor = 0.2
  )

  /** Conservative retry policy with longer delays. */
  val Conservative: RetryPolicy = RetryPolicy(
    maxRetries = 2,
    initialDelayMs = 5000L,
    maxDelayMs = 30000L,
    backoffMultiplier = 1.5,
    jitterFactor = 0.1
  )

  /**
   * Calculates the delay before the next retry attempt.
   *
   * @param policy The retry policy configuration
   * @param attemptNumber The current attempt number (0-indexed)
   * @param random Random instance for jitter calculation (defaults to scala.util.Random)
   * @return Delay in milliseconds before the next retry
   */
  def calculateDelay(policy: RetryPolicy, attemptNumber: Int, random: Random = Random): Long = {
    val baseDelay: Long = Math.min(
      (policy.initialDelayMs * Math.pow(policy.backoffMultiplier, attemptNumber.toDouble)).toLong,
      policy.maxDelayMs
    )
    val jitterRange: Double = baseDelay * policy.jitterFactor
    val jitter: Long        = (jitterRange * (random.nextDouble() * 2.0 - 1.0)).toLong
    Math.max(0L, baseDelay + jitter)
  }

  /**
   * Checks if an exception is retryable according to the policy.
   *
   * @param policy The retry policy configuration
   * @param error The exception to check
   * @return true if the exception should trigger a retry, false otherwise
   */
  def isRetryable(policy: RetryPolicy, error: Throwable): Boolean =
    if (policy.retryableExceptions.isEmpty) {
      true // Empty set means all exceptions are retryable
    } else {
      policy.retryableExceptions.contains(error.getClass.getName) ||
      policy.retryableExceptions.contains(error.getClass.getSimpleName)
    }
}
