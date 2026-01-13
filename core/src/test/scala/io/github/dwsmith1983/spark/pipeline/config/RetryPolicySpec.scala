package io.github.dwsmith1983.spark.pipeline.config

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class RetryPolicySpec extends AnyFunSpec with Matchers {

  describe("RetryPolicy") {

    describe("construction") {

      it("should create a policy with default values") {
        val policy: RetryPolicy = RetryPolicy()
        policy.maxRetries shouldBe 3
        policy.initialDelayMs shouldBe 1000L
        policy.maxDelayMs shouldBe 30000L
        policy.backoffMultiplier shouldBe 2.0
        policy.jitterFactor shouldBe 0.1
        policy.retryableExceptions shouldBe empty
      }

      it("should create a policy with custom values") {
        val policy: RetryPolicy = RetryPolicy(
          maxRetries = 5,
          initialDelayMs = 500L,
          maxDelayMs = 60000L,
          backoffMultiplier = 1.5,
          jitterFactor = 0.2,
          retryableExceptions = Set("java.io.IOException")
        )
        policy.maxRetries shouldBe 5
        policy.initialDelayMs shouldBe 500L
        policy.maxDelayMs shouldBe 60000L
        policy.backoffMultiplier shouldBe 1.5
        policy.jitterFactor shouldBe 0.2
        policy.retryableExceptions should contain("java.io.IOException")
      }

      it("should reject negative maxRetries") {
        an[IllegalArgumentException] should be thrownBy {
          RetryPolicy(maxRetries = -1)
        }
      }

      it("should reject negative initialDelayMs") {
        an[IllegalArgumentException] should be thrownBy {
          RetryPolicy(initialDelayMs = -1)
        }
      }

      it("should reject negative maxDelayMs") {
        an[IllegalArgumentException] should be thrownBy {
          RetryPolicy(maxDelayMs = -1)
        }
      }

      it("should reject backoffMultiplier less than 1.0") {
        an[IllegalArgumentException] should be thrownBy {
          RetryPolicy(backoffMultiplier = 0.5)
        }
      }

      it("should reject negative jitterFactor") {
        an[IllegalArgumentException] should be thrownBy {
          RetryPolicy(jitterFactor = -0.1)
        }
      }

      it("should reject jitterFactor greater than 1.0") {
        an[IllegalArgumentException] should be thrownBy {
          RetryPolicy(jitterFactor = 1.1)
        }
      }

      it("should allow maxRetries of 0") {
        val policy: RetryPolicy = RetryPolicy(maxRetries = 0)
        policy.maxRetries shouldBe 0
      }

      it("should allow jitterFactor of 0") {
        val policy: RetryPolicy = RetryPolicy(jitterFactor = 0.0)
        policy.jitterFactor shouldBe 0.0
      }

      it("should allow jitterFactor of 1.0") {
        val policy: RetryPolicy = RetryPolicy(jitterFactor = 1.0)
        policy.jitterFactor shouldBe 1.0
      }
    }

    describe("preset policies") {

      it("should have NoRetry with maxRetries = 0") {
        RetryPolicy.NoRetry.maxRetries shouldBe 0
      }

      it("should have Default with standard values") {
        val default: RetryPolicy = RetryPolicy.Default
        default.maxRetries shouldBe 3
        default.initialDelayMs shouldBe 1000L
      }

      it("should have Aggressive with more retries and shorter initial delay") {
        val aggressive: RetryPolicy = RetryPolicy.Aggressive
        aggressive.maxRetries shouldBe 5
        aggressive.initialDelayMs shouldBe 500L
      }

      it("should have Conservative with fewer retries and longer initial delay") {
        val conservative: RetryPolicy = RetryPolicy.Conservative
        conservative.maxRetries shouldBe 2
        conservative.initialDelayMs shouldBe 5000L
      }
    }
  }

  describe("RetryPolicy.calculateDelay") {

    it("should return initialDelayMs for attempt 0 with no jitter") {
      val policy: RetryPolicy = RetryPolicy(initialDelayMs = 1000L, jitterFactor = 0.0)
      val delay: Long         = RetryPolicy.calculateDelay(policy, 0)
      delay shouldBe 1000L
    }

    it("should apply exponential backoff") {
      val policy: RetryPolicy = RetryPolicy(
        initialDelayMs = 1000L,
        backoffMultiplier = 2.0,
        jitterFactor = 0.0
      )

      RetryPolicy.calculateDelay(policy, 0) shouldBe 1000L
      RetryPolicy.calculateDelay(policy, 1) shouldBe 2000L
      RetryPolicy.calculateDelay(policy, 2) shouldBe 4000L
      RetryPolicy.calculateDelay(policy, 3) shouldBe 8000L
    }

    it("should cap delay at maxDelayMs") {
      val policy: RetryPolicy = RetryPolicy(
        initialDelayMs = 1000L,
        maxDelayMs = 5000L,
        backoffMultiplier = 2.0,
        jitterFactor = 0.0
      )

      RetryPolicy.calculateDelay(policy, 0) shouldBe 1000L
      RetryPolicy.calculateDelay(policy, 1) shouldBe 2000L
      RetryPolicy.calculateDelay(policy, 2) shouldBe 4000L
      RetryPolicy.calculateDelay(policy, 3) shouldBe 5000L  // capped
      RetryPolicy.calculateDelay(policy, 10) shouldBe 5000L // still capped
    }

    it("should apply jitter within expected bounds") {
      val policy: RetryPolicy = RetryPolicy(
        initialDelayMs = 1000L,
        jitterFactor = 0.1
      )
      val seededRandom: Random = new Random(42)

      val delays: Seq[Long] = (0 until 100).map(_ => RetryPolicy.calculateDelay(policy, 0, seededRandom))

      delays.foreach { delay =>
        delay should be >= 900L  // 1000 - 10%
        delay should be <= 1100L // 1000 + 10%
      }
    }

    it("should produce varying delays with jitter") {
      val policy: RetryPolicy = RetryPolicy(initialDelayMs = 1000L, jitterFactor = 0.2)
      val delays: Set[Long]   = (0 until 10).map(_ => RetryPolicy.calculateDelay(policy, 0)).toSet
      delays.size should be > 1
    }

    it("should handle large backoff multiplier") {
      val policy: RetryPolicy = RetryPolicy(
        initialDelayMs = 100L,
        maxDelayMs = 10000L,
        backoffMultiplier = 10.0,
        jitterFactor = 0.0
      )

      RetryPolicy.calculateDelay(policy, 0) shouldBe 100L
      RetryPolicy.calculateDelay(policy, 1) shouldBe 1000L
      RetryPolicy.calculateDelay(policy, 2) shouldBe 10000L // capped
      RetryPolicy.calculateDelay(policy, 3) shouldBe 10000L // still capped
    }

    it("should never return negative delay") {
      val policy: RetryPolicy = RetryPolicy(initialDelayMs = 10L, jitterFactor = 1.0)
      val delays: Seq[Long]   = (0 until 1000).map(_ => RetryPolicy.calculateDelay(policy, 0))
      delays.foreach(_ should be >= 0L)
    }
  }

  describe("RetryPolicy.isRetryable") {

    it("should return true for any exception when retryableExceptions is empty") {
      val policy: RetryPolicy = RetryPolicy(retryableExceptions = Set.empty)

      RetryPolicy.isRetryable(policy, new RuntimeException("test")) shouldBe true
      RetryPolicy.isRetryable(policy, new IllegalArgumentException("test")) shouldBe true
      RetryPolicy.isRetryable(policy, new java.io.IOException("test")) shouldBe true
    }

    it("should return true when exception class name is in retryableExceptions") {
      val policy: RetryPolicy = RetryPolicy(
        retryableExceptions = Set("java.io.IOException", "java.net.SocketException")
      )

      RetryPolicy.isRetryable(policy, new java.io.IOException("test")) shouldBe true
      RetryPolicy.isRetryable(policy, new java.net.SocketException("test")) shouldBe true
    }

    it("should return true when exception simple name is in retryableExceptions") {
      val policy: RetryPolicy = RetryPolicy(
        retryableExceptions = Set("IOException", "SocketException")
      )

      RetryPolicy.isRetryable(policy, new java.io.IOException("test")) shouldBe true
      RetryPolicy.isRetryable(policy, new java.net.SocketException("test")) shouldBe true
    }

    it("should return false when exception is not in retryableExceptions") {
      val policy: RetryPolicy = RetryPolicy(
        retryableExceptions = Set("java.io.IOException")
      )

      RetryPolicy.isRetryable(policy, new RuntimeException("test")) shouldBe false
      RetryPolicy.isRetryable(policy, new IllegalArgumentException("test")) shouldBe false
    }

    it("should handle custom exception classes") {
      val policy: RetryPolicy = RetryPolicy(
        retryableExceptions = Set("IllegalStateException", "UnsupportedOperationException")
      )

      RetryPolicy.isRetryable(policy, new IllegalStateException("test")) shouldBe true
      RetryPolicy.isRetryable(policy, new UnsupportedOperationException("test")) shouldBe true
      RetryPolicy.isRetryable(policy, new RuntimeException("test")) shouldBe false
    }
  }

  describe("immutability") {

    it("should not be mutable") {
      val policy1: RetryPolicy = RetryPolicy(maxRetries = 3)
      val policy2: RetryPolicy = policy1.copy(maxRetries = 5)

      policy1.maxRetries shouldBe 3
      policy2.maxRetries shouldBe 5
    }
  }
}
