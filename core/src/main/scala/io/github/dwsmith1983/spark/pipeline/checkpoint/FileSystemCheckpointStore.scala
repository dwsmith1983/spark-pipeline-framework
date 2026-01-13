package io.github.dwsmith1983.spark.pipeline.checkpoint

import org.apache.logging.log4j.{LogManager, Logger}

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.util.stream.{Stream => JStream}
import scala.util.{Failure, Success, Try}

/**
 * File system-based checkpoint store.
 *
 * Stores checkpoints as JSON files in the specified base directory.
 * Uses atomic file operations (write to temp file, then rename) to
 * ensure consistency.
 *
 * == Directory Structure ==
 *
 * {{{
 * basePath/
 *   pipeline-name-1/
 *     run-id-1.json
 *     run-id-2.json
 *   pipeline-name-2/
 *     run-id-3.json
 * }}}
 *
 * == Example ==
 *
 * {{{
 * val store = FileSystemCheckpointStore("/tmp/spark-checkpoints")
 * store.save(checkpointState)
 * val state = store.loadLatest("my-pipeline")
 * }}}
 *
 * @param basePath Base directory for checkpoint storage
 */
class FileSystemCheckpointStore(basePath: String) extends CheckpointStore {

  private val logger: Logger = LogManager.getLogger(getClass)
  private val baseDir: Path  = Paths.get(basePath)
  private val jsonExtension  = ".json"

  // Ensure base directory exists
  Try(Files.createDirectories(baseDir)) match {
    case Success(_) =>
      logger.debug(s"Checkpoint store initialized at: $basePath")
    case Failure(e) =>
      logger.error(s"Failed to create checkpoint directory: $basePath", e)
      throw new CheckpointException(s"Cannot create checkpoint directory: $basePath", e)
  }

  override def save(state: CheckpointState): Unit = {
    val pipelineDir = baseDir.resolve(sanitizeName(state.pipelineName))
    val targetFile  = pipelineDir.resolve(state.runId + jsonExtension)
    val tempFile    = pipelineDir.resolve(s".${state.runId}.tmp")

    try {
      Files.createDirectories(pipelineDir)

      // Write to temp file first
      val json = CheckpointSerializer.toJson(state)
      Files.write(tempFile, json.getBytes(StandardCharsets.UTF_8))

      // Atomic rename
      Files.move(tempFile, targetFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)

      logger.debug(s"Checkpoint saved: ${state.runId} at ${targetFile.toAbsolutePath}")
    } catch {
      case e: IOException =>
        // Clean up temp file if it exists
        Try(Files.deleteIfExists(tempFile))
        throw CheckpointException.saveFailure(state.runId, e)
    }
  }

  override def loadLatest(pipelineName: String): Option[CheckpointState] = {
    val pipelineDir = baseDir.resolve(sanitizeName(pipelineName))

    if (!Files.exists(pipelineDir)) {
      return None
    }

    try {
      val checkpoints = listJsonFiles(pipelineDir)
        .flatMap(p => loadFromPath(p).toOption)
        .filter(_.status.isInstanceOf[CheckpointStatus.Failed])

      // Return the most recent by startedAt
      checkpoints.sortBy(_.startedAt)(Ordering[java.time.Instant].reverse).headOption
    } catch {
      case e: IOException =>
        logger.warn(s"Error listing checkpoints for pipeline: $pipelineName", e)
        None
    }
  }

  override def loadByRunId(runId: String): Option[CheckpointState] =
    // Search all pipeline directories for the runId
    try {
      listDirectories(baseDir)
        .map(_.resolve(runId + jsonExtension))
        .find(Files.exists(_))
        .flatMap(loadFromPath(_).toOption)
    } catch {
      case e: IOException =>
        logger.warn(s"Error searching for checkpoint: $runId", e)
        None
    }

  override def delete(runId: String): Unit =
    try {
      // Find and delete the checkpoint file
      listDirectories(baseDir)
        .map(_.resolve(runId + jsonExtension))
        .filter(Files.exists(_))
        .foreach { path =>
          Files.delete(path)
          logger.debug(s"Checkpoint deleted: $runId")
        }
    } catch {
      case e: IOException =>
        throw CheckpointException.deleteFailure(runId, e)
    }

  override def listIncomplete(): List[CheckpointState] =
    try {
      listDirectories(baseDir)
        .flatMap { pipelineDir =>
          listJsonFiles(pipelineDir)
            .flatMap(loadFromPath(_).toOption)
            .filter(_.status.isInstanceOf[CheckpointStatus.Failed])
        }
        .sortBy(_.startedAt)(Ordering[java.time.Instant].reverse)
    } catch {
      case e: IOException =>
        logger.warn("Error listing incomplete checkpoints", e)
        List.empty
    }

  override def listIncomplete(pipelineName: String): List[CheckpointState] = {
    val pipelineDir = baseDir.resolve(sanitizeName(pipelineName))

    if (!Files.exists(pipelineDir)) {
      return List.empty
    }

    try {
      listJsonFiles(pipelineDir)
        .flatMap(loadFromPath(_).toOption)
        .filter(_.status.isInstanceOf[CheckpointStatus.Failed])
        .sortBy(_.startedAt)(Ordering[java.time.Instant].reverse)
    } catch {
      case e: IOException =>
        logger.warn(s"Error listing incomplete checkpoints for: $pipelineName", e)
        List.empty
    }
  }

  private def loadFromPath(path: Path): Try[CheckpointState] =
    Try {
      val json = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
      CheckpointSerializer.fromJson(json)
    }

  /** Lists all subdirectories of the given directory. */
  private def listDirectories(dir: Path): List[Path] =
    withStream(Files.list(dir))(stream => streamToList(stream).filter(Files.isDirectory(_)))

  /** Lists all JSON files in the given directory (excluding hidden files). */
  private def listJsonFiles(dir: Path): List[Path] =
    withStream(Files.list(dir)) { stream =>
      streamToList(stream).filter { p =>
        val name = p.getFileName.toString
        name.endsWith(jsonExtension) && !name.startsWith(".")
      }
    }

  /** Converts a Java Stream to a Scala List, ensuring the stream is closed. */
  private def withStream[T, R](stream: JStream[T])(f: JStream[T] => R): R =
    try {
      f(stream)
    } finally {
      stream.close()
    }

  /** Converts a Java Stream to a Scala List. */
  private def streamToList[T](stream: JStream[T]): List[T] = {
    val builder = List.newBuilder[T]
    stream.forEach(item => builder += item)
    builder.result()
  }

  /**
   * Sanitize pipeline name for use as directory name.
   * Replaces problematic characters with underscores.
   */
  private def sanitizeName(name: String): String =
    name.replaceAll("[^a-zA-Z0-9._-]", "_")
}

object FileSystemCheckpointStore {

  /**
   * Creates a checkpoint store at the specified path.
   *
   * @param basePath Base directory for checkpoint storage
   * @return New FileSystemCheckpointStore instance
   */
  def apply(basePath: String): FileSystemCheckpointStore =
    new FileSystemCheckpointStore(basePath)

  /** Default checkpoint location in the system temp directory. */
  val DefaultPath: String = System.getProperty("java.io.tmpdir") + "/spark-pipeline-checkpoints"

  /**
   * Creates a checkpoint store at the default location.
   *
   * @return New FileSystemCheckpointStore at default path
   */
  def default(): FileSystemCheckpointStore =
    new FileSystemCheckpointStore(DefaultPath)
}
