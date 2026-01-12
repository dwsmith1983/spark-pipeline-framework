package io.github.dwsmith1983.spark.pipeline.secrets

import scala.collection.concurrent.TrieMap

/**
 * TTL-based cache for resolved secrets.
 *
 * Prevents repeated calls to secrets backends during configuration parsing.
 * Thread-safe for concurrent access.
 *
 * @param ttlMillis Time-to-live for cached entries in milliseconds (default: 5 minutes)
 */
class SecretsCache(ttlMillis: Long = SecretsCache.DefaultTtlMillis) {

  private case class CachedEntry(value: String, expiresAt: Long)

  private val cache: TrieMap[String, CachedEntry] = TrieMap.empty

  /**
   * Get a cached secret value if present and not expired.
   *
   * @param key Cache key (typically provider + path + key)
   * @return Some(value) if cached and valid, None otherwise
   */
  def get(key: String): Option[String] = {
    cache.get(key).flatMap { entry =>
      if (System.currentTimeMillis() < entry.expiresAt) {
        Some(entry.value)
      } else {
        val _ = cache.remove(key)
        None
      }
    }
  }

  /**
   * Store a secret value in the cache.
   *
   * @param key Cache key
   * @param value Secret value to cache
   */
  def put(key: String, value: String): Unit = {
    val expiresAt = System.currentTimeMillis() + ttlMillis
    val _ = cache.put(key, CachedEntry(value, expiresAt))
  }

  /**
   * Get a value from cache or compute it.
   *
   * If the key is not in cache or expired, the compute function is called
   * and the result is cached.
   *
   * @param key Cache key
   * @param compute Function to compute the value if not cached
   * @return Tuple of (value, wasCached)
   */
  def getOrCompute(key: String)(compute: => String): (String, Boolean) =
    get(key) match {
      case Some(value) => (value, true)
      case None =>
        val value = compute
        put(key, value)
        (value, false)
    }

  /**
   * Invalidate a specific cache entry.
   *
   * @param key Cache key to invalidate
   */
  def invalidate(key: String): Unit = {
    val _ = cache.remove(key)
  }

  /**
   * Invalidate all cache entries.
   */
  def invalidateAll(): Unit =
    cache.clear()

  /**
   * Get current cache size (including potentially expired entries).
   *
   * @return Number of entries in cache
   */
  def size: Int = cache.size

  /**
   * Build a cache key from reference components.
   *
   * @param provider Provider scheme
   * @param path Secret path
   * @param key Optional key within secret
   * @return Canonical cache key
   */
  def buildKey(provider: String, path: String, key: Option[String]): String =
    key match {
      case Some(k) => s"$provider://$path#$k"
      case None    => s"$provider://$path"
    }
}

object SecretsCache {

  /** Default cache TTL: 5 minutes */
  val DefaultTtlMillis: Long = 5L * 60L * 1000L

  /** Create a cache with default TTL */
  def apply(): SecretsCache = new SecretsCache()

  /** Create a cache with custom TTL in seconds */
  def withTtlSeconds(seconds: Long): SecretsCache =
    new SecretsCache(seconds * 1000L)

  /** Create a disabled cache (0 TTL) */
  def disabled(): SecretsCache = new SecretsCache(0L)
}
