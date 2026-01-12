package io.github.dwsmith1983.spark.pipeline.config

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class CircuitBreakerSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  describe("CircuitBreakerConfig") {

    describe("construction") {

      it("should create config with default values") {
        val config: CircuitBreakerConfig = CircuitBreakerConfig()
        config.failureThreshold shouldBe 5
        config.resetTimeoutMs shouldBe 60000L
        config.halfOpenSuccessThreshold shouldBe 2
      }

      it("should create config with custom values") {
        val config: CircuitBreakerConfig = CircuitBreakerConfig(
          failureThreshold = 3,
          resetTimeoutMs = 30000L,
          halfOpenSuccessThreshold = 1
        )
        config.failureThreshold shouldBe 3
        config.resetTimeoutMs shouldBe 30000L
        config.halfOpenSuccessThreshold shouldBe 1
      }

      it("should reject non-positive failureThreshold") {
        an[IllegalArgumentException] should be thrownBy {
          CircuitBreakerConfig(failureThreshold = 0)
        }
        an[IllegalArgumentException] should be thrownBy {
          CircuitBreakerConfig(failureThreshold = -1)
        }
      }

      it("should reject non-positive resetTimeoutMs") {
        an[IllegalArgumentException] should be thrownBy {
          CircuitBreakerConfig(resetTimeoutMs = 0)
        }
      }

      it("should reject non-positive halfOpenSuccessThreshold") {
        an[IllegalArgumentException] should be thrownBy {
          CircuitBreakerConfig(halfOpenSuccessThreshold = 0)
        }
      }
    }

    describe("preset configs") {

      it("should have Default config") {
        val default: CircuitBreakerConfig = CircuitBreakerConfig.Default
        default.failureThreshold shouldBe 5
      }

      it("should have Sensitive config with lower threshold") {
        val sensitive: CircuitBreakerConfig = CircuitBreakerConfig.Sensitive
        sensitive.failureThreshold shouldBe 3
        sensitive.resetTimeoutMs shouldBe 120000L
      }

      it("should have Resilient config with higher threshold") {
        val resilient: CircuitBreakerConfig = CircuitBreakerConfig.Resilient
        resilient.failureThreshold shouldBe 10
        resilient.resetTimeoutMs shouldBe 30000L
      }
    }
  }

  describe("CircuitState") {

    it("should have correct names") {
      CircuitState.Closed.name shouldBe "closed"
      CircuitState.Open.name shouldBe "open"
      CircuitState.HalfOpen.name shouldBe "half_open"
    }
  }

  describe("CircuitBreaker") {

    describe("initial state") {

      it("should start in Closed state") {
        val cb: CircuitBreaker = new CircuitBreaker("test", CircuitBreakerConfig.Default)
        cb.currentState shouldBe CircuitState.Closed
      }

      it("should start with zero failure count") {
        val cb: CircuitBreaker = new CircuitBreaker("test", CircuitBreakerConfig.Default)
        cb.currentFailureCount shouldBe 0
      }

      it("should start with zero success count") {
        val cb: CircuitBreaker = new CircuitBreaker("test", CircuitBreakerConfig.Default)
        cb.currentSuccessCount shouldBe 0
      }

      it("should allow execution in initial state") {
        val cb: CircuitBreaker = new CircuitBreaker("test", CircuitBreakerConfig.Default)
        cb.canExecute() shouldBe true
      }
    }

    describe("Closed state behavior") {

      it("should allow execution") {
        val cb: CircuitBreaker = new CircuitBreaker("test", CircuitBreakerConfig.Default)
        cb.canExecute() shouldBe true
      }

      it("should track failures") {
        val cb: CircuitBreaker = new CircuitBreaker("test", CircuitBreakerConfig.Default)
        cb.recordFailure()
        cb.currentFailureCount shouldBe 1
        cb.recordFailure()
        cb.currentFailureCount shouldBe 2
      }

      it("should reset failure count on success") {
        val cb: CircuitBreaker = new CircuitBreaker("test", CircuitBreakerConfig.Default)
        cb.recordFailure()
        cb.recordFailure()
        cb.currentFailureCount shouldBe 2

        cb.recordSuccess()
        cb.currentFailureCount shouldBe 0
      }

      it("should open circuit when failure threshold is reached") {
        val config: CircuitBreakerConfig = CircuitBreakerConfig(failureThreshold = 3)
        val cb: CircuitBreaker           = new CircuitBreaker("test", config)

        cb.recordFailure()
        cb.currentState shouldBe CircuitState.Closed
        cb.recordFailure()
        cb.currentState shouldBe CircuitState.Closed
        cb.recordFailure()
        cb.currentState shouldBe CircuitState.Open
      }
    }

    describe("Open state behavior") {

      it("should reject execution") {
        val config: CircuitBreakerConfig = CircuitBreakerConfig(failureThreshold = 1)
        val cb: CircuitBreaker           = new CircuitBreaker("test", config)

        cb.recordFailure()
        cb.currentState shouldBe CircuitState.Open
        cb.canExecute() shouldBe false
      }

      it("should transition to HalfOpen after timeout") {
        var currentTime: Long            = 0L
        val config: CircuitBreakerConfig = CircuitBreakerConfig(failureThreshold = 1, resetTimeoutMs = 1000L)
        val cb: CircuitBreaker           = new CircuitBreaker("test", config, () => currentTime)

        cb.recordFailure()
        cb.currentState shouldBe CircuitState.Open
        cb.canExecute() shouldBe false

        // Advance time past timeout
        currentTime = 1001L
        cb.canExecute() shouldBe true
        cb.currentState shouldBe CircuitState.HalfOpen
      }

      it("should not transition before timeout") {
        var currentTime: Long            = 0L
        val config: CircuitBreakerConfig = CircuitBreakerConfig(failureThreshold = 1, resetTimeoutMs = 1000L)
        val cb: CircuitBreaker           = new CircuitBreaker("test", config, () => currentTime)

        cb.recordFailure()
        cb.currentState shouldBe CircuitState.Open

        // Advance time but not past timeout
        currentTime = 999L
        cb.canExecute() shouldBe false
        cb.currentState shouldBe CircuitState.Open
      }
    }

    describe("HalfOpen state behavior") {

      it("should allow execution") {
        var currentTime: Long            = 0L
        val config: CircuitBreakerConfig = CircuitBreakerConfig(failureThreshold = 1, resetTimeoutMs = 100L)
        val cb: CircuitBreaker           = new CircuitBreaker("test", config, () => currentTime)

        cb.recordFailure()
        currentTime = 101L
        cb.canExecute() shouldBe true
        cb.currentState shouldBe CircuitState.HalfOpen
      }

      it("should close circuit after success threshold is reached") {
        var currentTime: Long = 0L
        val config: CircuitBreakerConfig = CircuitBreakerConfig(
          failureThreshold = 1,
          resetTimeoutMs = 100L,
          halfOpenSuccessThreshold = 2
        )
        val cb: CircuitBreaker = new CircuitBreaker("test", config, () => currentTime)

        cb.recordFailure()
        currentTime = 101L
        cb.canExecute()
        cb.currentState shouldBe CircuitState.HalfOpen

        cb.recordSuccess()
        cb.currentState shouldBe CircuitState.HalfOpen
        cb.currentSuccessCount shouldBe 1

        cb.recordSuccess()
        cb.currentState shouldBe CircuitState.Closed
      }

      it("should open circuit immediately on failure") {
        var currentTime: Long = 0L
        val config: CircuitBreakerConfig = CircuitBreakerConfig(
          failureThreshold = 5,
          resetTimeoutMs = 100L,
          halfOpenSuccessThreshold = 3
        )
        val cb: CircuitBreaker = new CircuitBreaker("test", config, () => currentTime)

        // Get to Open state
        (1 to 5).foreach(_ => cb.recordFailure())
        cb.currentState shouldBe CircuitState.Open

        // Transition to HalfOpen
        currentTime = 101L
        cb.canExecute()
        cb.currentState shouldBe CircuitState.HalfOpen

        // One failure should re-open
        cb.recordFailure()
        cb.currentState shouldBe CircuitState.Open
      }
    }

    describe("reset") {

      it("should return to Closed state") {
        val config: CircuitBreakerConfig = CircuitBreakerConfig(failureThreshold = 1)
        val cb: CircuitBreaker           = new CircuitBreaker("test", config)

        cb.recordFailure()
        cb.currentState shouldBe CircuitState.Open

        cb.reset()
        cb.currentState shouldBe CircuitState.Closed
      }

      it("should clear failure count") {
        val config: CircuitBreakerConfig = CircuitBreakerConfig(failureThreshold = 5)
        val cb: CircuitBreaker           = new CircuitBreaker("test", config)

        cb.recordFailure()
        cb.recordFailure()
        cb.currentFailureCount shouldBe 2

        cb.reset()
        cb.currentFailureCount shouldBe 0
      }
    }

    describe("factory methods") {

      it("should create with component name only") {
        val cb: CircuitBreaker = CircuitBreaker("my-component")
        cb.componentName shouldBe "my-component"
        cb.config shouldBe CircuitBreakerConfig.Default
      }

      it("should create with component name and config") {
        val config: CircuitBreakerConfig = CircuitBreakerConfig(failureThreshold = 10)
        val cb: CircuitBreaker           = CircuitBreaker("my-component", config)
        cb.componentName shouldBe "my-component"
        cb.config.failureThreshold shouldBe 10
      }
    }

    describe("state transitions logging") {

      it("should track component name") {
        val cb: CircuitBreaker = new CircuitBreaker("test-component", CircuitBreakerConfig.Default)
        cb.componentName shouldBe "test-component"
      }
    }
  }

  describe("CircuitBreakerOpenException") {

    it("should contain component name") {
      val ex: CircuitBreakerOpenException =
        new CircuitBreakerOpenException("my-component", CircuitState.Open)
      ex.componentName shouldBe "my-component"
    }

    it("should contain circuit state") {
      val ex: CircuitBreakerOpenException =
        new CircuitBreakerOpenException("my-component", CircuitState.Open)
      ex.state shouldBe CircuitState.Open
    }

    it("should have descriptive message") {
      val ex: CircuitBreakerOpenException =
        new CircuitBreakerOpenException("my-component", CircuitState.Open)
      ex.getMessage should include("my-component")
      ex.getMessage should include("open")
    }
  }
}
