package io.github.dwsmith1983.spark.pipeline.secrets.audit

import io.github.dwsmith1983.spark.pipeline.secrets.SecretsReference
import org.apache.logging.log4j.{LogManager, Logger}

import java.time.Instant

/**
 * Audit event for secret access.
 *
 * Contains metadata about the access but NEVER the secret value itself.
 *
 * @param timestamp When the access occurred
 * @param provider The secrets provider used
 * @param path Path to the secret
 * @param key Optional key within the secret
 * @param cached Whether the value came from cache
 * @param success Whether the resolution succeeded
 * @param errorMessage Error message if resolution failed
 */
case class SecretAccessEvent(
  timestamp: Instant,
  provider: String,
  path: String,
  key: Option[String],
  cached: Boolean,
  success: Boolean,
  errorMessage: Option[String] = None)

/**
 * Trait for audit logging of secret access.
 *
 * Implementations should log access attempts without exposing secret values.
 * This enables security auditing and debugging without compromising secrets.
 */
trait SecretsAuditLogger {

  /**
   * Log a secret access event.
   *
   * @param event The access event to log
   */
  def logAccess(event: SecretAccessEvent): Unit

  /**
   * Create an access event from a reference and resolution result.
   *
   * @param ref The secret reference
   * @param cached Whether the value was from cache
   * @return Access event
   */
  def createAccessEvent(ref: SecretsReference, cached: Boolean): SecretAccessEvent =
    SecretAccessEvent(
      timestamp = Instant.now(),
      provider = ref.provider,
      path = ref.path,
      key = ref.key,
      cached = cached,
      success = true
    )

  /**
   * Create an error event from a reference and failure.
   *
   * @param ref The secret reference
   * @param error The error that occurred
   * @return Access event with error details
   */
  def createErrorEvent(ref: SecretsReference, error: Throwable): SecretAccessEvent =
    SecretAccessEvent(
      timestamp = Instant.now(),
      provider = ref.provider,
      path = ref.path,
      key = ref.key,
      cached = false,
      success = false,
      errorMessage = Some(error.getMessage)
    )
}

/**
 * Default audit logger using Log4j2.
 *
 * Logs at INFO level for successful accesses and WARN for failures.
 * Uses a dedicated logger name for easy filtering in log configuration.
 */
class Log4jSecretsAuditLogger extends SecretsAuditLogger {

  private val logger: Logger = LogManager.getLogger("secrets.audit")

  override def logAccess(event: SecretAccessEvent): Unit = {
    val keyPart   = event.key.map(k => s"#$k").getOrElse("")
    val cachePart = if (event.cached) " (cached)" else ""
    val location  = s"${event.provider}://${event.path}$keyPart"

    if (event.success) {
      val message = s"Secret accessed: $location at ${event.timestamp}$cachePart"
      logger.info(message)
    } else {
      val errorMsg = event.errorMessage.getOrElse("unknown error")
      val message  = s"Secret access failed: $location at ${event.timestamp} - $errorMsg"
      logger.warn(message)
    }
  }
}

/** No-op audit logger for testing or when auditing is disabled. */
object NoOpSecretsAuditLogger extends SecretsAuditLogger {
  override def logAccess(event: SecretAccessEvent): Unit = ()
}

/** Audit logger that collects events in memory for testing. */
class InMemorySecretsAuditLogger extends SecretsAuditLogger {
  private var events: List[SecretAccessEvent] = List.empty

  override def logAccess(event: SecretAccessEvent): Unit =
    synchronized {
      events = event :: events
    }

  def getEvents: List[SecretAccessEvent] =
    synchronized {
      events.reverse
    }

  def clear(): Unit =
    synchronized {
      events = List.empty
    }
}

object SecretsAuditLogger {

  /** Default logger using Log4j2 */
  def default: SecretsAuditLogger = new Log4jSecretsAuditLogger

  /** No-op logger for when auditing is disabled */
  def noOp: SecretsAuditLogger = NoOpSecretsAuditLogger

  /** In-memory logger for testing */
  def inMemory: InMemorySecretsAuditLogger = new InMemorySecretsAuditLogger
}
