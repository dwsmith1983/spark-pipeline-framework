package io.github.dwsmith1983.spark.pipeline.secrets

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success}

class SecretsReferenceSpec extends AnyFunSpec with Matchers {

  describe("SecretsReference.parse") {

    it("should parse AWS reference with key") {
      val ref = "${secret:aws://my-app/db-creds#password}"
      SecretsReference.parse(ref) shouldBe Success(
        SecretsReference(
          provider = "aws",
          path = "my-app/db-creds",
          key = Some("password"),
          original = ref
        )
      )
    }

    it("should parse AWS reference without key") {
      val ref = "${secret:aws://my-app/connection-string}"
      SecretsReference.parse(ref) shouldBe Success(
        SecretsReference(
          provider = "aws",
          path = "my-app/connection-string",
          key = None,
          original = ref
        )
      )
    }

    it("should parse Vault reference with key") {
      val ref = "${secret:vault://secret/data/database/prod#username}"
      SecretsReference.parse(ref) shouldBe Success(
        SecretsReference(
          provider = "vault",
          path = "secret/data/database/prod",
          key = Some("username"),
          original = ref
        )
      )
    }

    it("should parse env reference") {
      val ref = "${secret:env://DB_PASSWORD}"
      SecretsReference.parse(ref) shouldBe Success(
        SecretsReference(
          provider = "env",
          path = "DB_PASSWORD",
          key = None,
          original = ref
        )
      )
    }

    it("should trim whitespace from path and key") {
      val ref    = "${secret:aws://  my-path  #  my-key  }"
      val result = SecretsReference.parse(ref)
      result.isSuccess shouldBe true
      result.get.path shouldBe "my-path"
      result.get.key shouldBe Some("my-key")
    }

    it("should fail on invalid format") {
      val ref = "not-a-secret-reference"
      SecretsReference.parse(ref).isFailure shouldBe true
    }

    it("should fail on missing provider") {
      val ref = "${secret://path}"
      SecretsReference.parse(ref).isFailure shouldBe true
    }
  }

  describe("SecretsReference.findAll") {

    it("should find all references in config") {
      val config =
        """
          |database {
          |  username = "${secret:aws://db/creds#username}"
          |  password = "${secret:aws://db/creds#password}"
          |  host = "localhost"
          |}
          |""".stripMargin

      val refs = SecretsReference.findAll(config)
      refs should have size 2
      refs should contain("${secret:aws://db/creds#username}")
      refs should contain("${secret:aws://db/creds#password}")
    }

    it("should return empty list when no references") {
      val config = """database { host = "localhost" }"""
      SecretsReference.findAll(config) shouldBe empty
    }

    it("should find mixed provider references") {
      val config =
        """
          |api-key = "${secret:env://API_KEY}"
          |db-pass = "${secret:vault://database/creds#password}"
          |""".stripMargin

      val refs = SecretsReference.findAll(config)
      refs should have size 2
    }
  }

  describe("SecretsReference.containsReferences") {

    it("should return true when references present") {
      val config = """password = "${secret:env://DB_PASS}""""
      SecretsReference.containsReferences(config) shouldBe true
    }

    it("should return false when no references") {
      val config = """password = "hardcoded""""
      SecretsReference.containsReferences(config) shouldBe false
    }
  }

  describe("SecretsReference.parseAll") {

    it("should parse all valid references") {
      val config =
        """
          |a = "${secret:aws://path1#key1}"
          |b = "${secret:vault://path2#key2}"
          |""".stripMargin

      val refs = SecretsReference.parseAll(config)
      refs should have size 2
      refs.map(_.provider) should contain theSameElementsAs Seq("aws", "vault")
    }
  }
}
