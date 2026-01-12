package io.github.dwsmith1983.spark.pipeline.secrets

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SecretsCacheSpec extends AnyFunSpec with Matchers {

  describe("SecretsCache") {

    it("should store and retrieve values") {
      val cache = SecretsCache()
      cache.put("key1", "value1")
      cache.get("key1") shouldBe Some("value1")
    }

    it("should return None for missing keys") {
      val cache = SecretsCache()
      cache.get("nonexistent") shouldBe None
    }

    it("should expire entries after TTL") {
      val cache = new SecretsCache(ttlMillis = 50)
      cache.put("key1", "value1")
      cache.get("key1") shouldBe Some("value1")

      Thread.sleep(100)

      cache.get("key1") shouldBe None
    }

    it("should invalidate specific entries") {
      val cache = SecretsCache()
      cache.put("key1", "value1")
      cache.put("key2", "value2")

      cache.invalidate("key1")

      cache.get("key1") shouldBe None
      cache.get("key2") shouldBe Some("value2")
    }

    it("should invalidate all entries") {
      val cache = SecretsCache()
      cache.put("key1", "value1")
      cache.put("key2", "value2")

      cache.invalidateAll()

      cache.get("key1") shouldBe None
      cache.get("key2") shouldBe None
    }

    it("should report correct size") {
      val cache = SecretsCache()
      cache.size shouldBe 0

      cache.put("key1", "value1")
      cache.size shouldBe 1

      cache.put("key2", "value2")
      cache.size shouldBe 2
    }

    it("should compute and cache values with getOrCompute") {
      val cache = SecretsCache()
      var computeCount = 0

      val (value1, cached1) = cache.getOrCompute("key1") {
        computeCount += 1
        "computed"
      }

      value1 shouldBe "computed"
      cached1 shouldBe false
      computeCount shouldBe 1

      val (value2, cached2) = cache.getOrCompute("key1") {
        computeCount += 1
        "computed-again"
      }

      value2 shouldBe "computed"
      cached2 shouldBe true
      computeCount shouldBe 1
    }

    it("should build consistent cache keys") {
      val cache = SecretsCache()

      val key1 = cache.buildKey("aws", "path/to/secret", Some("mykey"))
      val key2 = cache.buildKey("aws", "path/to/secret", Some("mykey"))
      key1 shouldBe key2
      key1 shouldBe "aws://path/to/secret#mykey"

      val key3 = cache.buildKey("vault", "secret/path", None)
      key3 shouldBe "vault://secret/path"
    }
  }

  describe("SecretsCache factory methods") {

    it("should create default cache") {
      val cache = SecretsCache()
      cache should not be null
    }

    it("should create cache with custom TTL in seconds") {
      val cache = SecretsCache.withTtlSeconds(10)
      cache.put("key", "value")
      cache.get("key") shouldBe Some("value")
    }

    it("should create disabled cache with 0 TTL") {
      val cache = SecretsCache.disabled()
      cache.put("key", "value")
      cache.get("key") shouldBe None
    }
  }
}
