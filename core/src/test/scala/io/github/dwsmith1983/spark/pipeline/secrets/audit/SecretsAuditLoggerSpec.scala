package io.github.dwsmith1983.spark.pipeline.secrets.audit

import io.github.dwsmith1983.spark.pipeline.secrets.SecretsReference
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class SecretsAuditLoggerSpec extends AnyFunSpec with Matchers {

  describe("SecretAccessEvent") {

    it("should store all access metadata") {
      val event = SecretAccessEvent(
        timestamp = Instant.now(),
        provider = "aws",
        path = "prod/database",
        key = Some("password"),
        cached = false,
        success = true
      )

      event.provider shouldBe "aws"
      event.path shouldBe "prod/database"
      event.key shouldBe Some("password")
      event.cached shouldBe false
      event.success shouldBe true
      event.errorMessage shouldBe None
    }

    it("should store error details on failure") {
      val event = SecretAccessEvent(
        timestamp = Instant.now(),
        provider = "vault",
        path = "secret/missing",
        key = None,
        cached = false,
        success = false,
        errorMessage = Some("Secret not found")
      )

      event.success shouldBe false
      event.errorMessage shouldBe Some("Secret not found")
    }
  }

  describe("InMemorySecretsAuditLogger") {

    it("should collect events in order") {
      val logger = new InMemorySecretsAuditLogger

      val event1 = SecretAccessEvent(
        Instant.now(), "env", "VAR1", None, false, true
      )
      val event2 = SecretAccessEvent(
        Instant.now(), "aws", "secret1", Some("key"), true, true
      )

      logger.logAccess(event1)
      logger.logAccess(event2)

      val events = logger.getEvents
      events should have size 2
      events(0).provider shouldBe "env"
      events(1).provider shouldBe "aws"
    }

    it("should clear events") {
      val logger = new InMemorySecretsAuditLogger

      logger.logAccess(SecretAccessEvent(
        Instant.now(), "env", "VAR", None, false, true
      ))
      logger.getEvents should have size 1

      logger.clear()
      logger.getEvents shouldBe empty
    }
  }

  describe("SecretsAuditLogger trait helpers") {

    it("should create access event from reference") {
      val logger = new InMemorySecretsAuditLogger
      val ref = SecretsReference("aws", "prod/db", Some("password"), "original")

      val event = logger.createAccessEvent(ref, cached = true)

      event.provider shouldBe "aws"
      event.path shouldBe "prod/db"
      event.key shouldBe Some("password")
      event.cached shouldBe true
      event.success shouldBe true
    }

    it("should create error event from reference and exception") {
      val logger = new InMemorySecretsAuditLogger
      val ref = SecretsReference("vault", "secret/path", None, "original")
      val error = new RuntimeException("Connection failed")

      val event = logger.createErrorEvent(ref, error)

      event.provider shouldBe "vault"
      event.path shouldBe "secret/path"
      event.key shouldBe None
      event.cached shouldBe false
      event.success shouldBe false
      event.errorMessage shouldBe Some("Connection failed")
    }
  }

  describe("NoOpSecretsAuditLogger") {

    it("should not throw on logAccess") {
      val logger = SecretsAuditLogger.noOp

      noException should be thrownBy {
        logger.logAccess(SecretAccessEvent(
          Instant.now(), "env", "VAR", None, false, true
        ))
      }
    }
  }

  describe("SecretsAuditLogger factory methods") {

    it("should create default logger") {
      val logger = SecretsAuditLogger.default
      logger shouldBe a[Log4jSecretsAuditLogger]
    }

    it("should create no-op logger") {
      SecretsAuditLogger.noOp shouldBe NoOpSecretsAuditLogger
    }

    it("should create in-memory logger") {
      val logger = SecretsAuditLogger.inMemory
      logger shouldBe a[InMemorySecretsAuditLogger]
    }
  }
}
