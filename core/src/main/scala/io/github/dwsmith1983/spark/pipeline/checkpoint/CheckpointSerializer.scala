package io.github.dwsmith1983.spark.pipeline.checkpoint

import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}

/**
 * JSON serialization for checkpoint state.
 *
 * Uses manual serialization to avoid external dependencies and
 * ensure compatibility across Scala 2.12/2.13.
 */
object CheckpointSerializer {

  private val timestampFormatter: DateTimeFormatter = DateTimeFormatter.ISO_INSTANT

  /**
   * Serialize checkpoint state to JSON.
   *
   * @param state The checkpoint state to serialize
   * @return JSON string representation
   */
  def toJson(state: CheckpointState): String = {
    val sb = new StringBuilder
    sb.append("{")
    sb.append(s""""runId":"${escapeJson(state.runId)}",""")
    sb.append(s""""pipelineName":"${escapeJson(state.pipelineName)}",""")
    sb.append(s""""startedAt":"${timestampFormatter.format(state.startedAt)}",""")
    sb.append(s""""completedComponents":${componentListToJson(state.completedComponents)},""")
    sb.append(s""""lastCheckpointedIndex":${state.lastCheckpointedIndex},""")
    sb.append(s""""totalComponents":${state.totalComponents},""")
    sb.append(s""""status":${statusToJson(state.status)}""")
    state.resumedFrom.foreach(r => sb.append(s""","resumedFrom":"${escapeJson(r)}""""))
    sb.append("}")
    sb.toString()
  }

  /**
   * Deserialize checkpoint state from JSON.
   *
   * @param json The JSON string to parse
   * @return Parsed checkpoint state
   * @note Throws `CheckpointException` if parsing fails
   */
  def fromJson(json: String): CheckpointState =
    Try {
      val fields     = parseObject(json)
      val runId      = fields("runId")
      val pipeline   = fields("pipelineName")
      val startedAt  = Instant.parse(fields("startedAt"))
      val components = parseComponentList(fields("completedComponents"))
      val lastIndex  = fields("lastCheckpointedIndex").toInt
      val total      = fields("totalComponents").toInt
      val status     = parseStatus(fields("status"))
      val resumed    = fields.get("resumedFrom").filter(_.nonEmpty)

      CheckpointState(runId, pipeline, startedAt, components, lastIndex, total, status, resumed)
    } match {
      case Success(state) => state
      case Failure(e)     => throw CheckpointException.corruptedCheckpoint("unknown", e)
    }

  private def escapeJson(s: String): String =
    if (s == null) {
      ""
    } else {
      s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("\b", "\\b")
        .replace("\f", "\\f")
    }

  private def unescapeJson(s: String): String = {
    // Use a placeholder for escaped backslashes to avoid interfering with other escape sequences
    val placeholder = "\u0000BACKSLASH\u0000"
    s.replace("\\\\", placeholder)
      .replace("\\n", "\n")
      .replace("\\r", "\r")
      .replace("\\t", "\t")
      .replace("\\b", "\b")
      .replace("\\f", "\f")
      .replace("\\\"", "\"")
      .replace(placeholder, "\\")
  }

  private def componentToJson(c: ComponentCheckpoint): String = {
    val sb = new StringBuilder
    sb.append("{")
    sb.append(s""""index":${c.index},""")
    sb.append(s""""name":"${escapeJson(c.name)}",""")
    sb.append(s""""instanceType":"${escapeJson(c.instanceType)}",""")
    sb.append(s""""startedAt":"${timestampFormatter.format(c.startedAt)}",""")
    sb.append(s""""completedAt":"${timestampFormatter.format(c.completedAt)}",""")
    sb.append(s""""durationMs":${c.durationMs}""")
    sb.append("}")
    sb.toString()
  }

  private def componentListToJson(components: List[ComponentCheckpoint]): String =
    components.map(componentToJson).mkString("[", ",", "]")

  private def statusToJson(status: CheckpointStatus): String = status match {
    case CheckpointStatus.Running =>
      """{"type":"Running"}"""
    case CheckpointStatus.Completed =>
      """{"type":"Completed"}"""
    case CheckpointStatus.Failed(msg, idx) =>
      s"""{"type":"Failed","errorMessage":"${escapeJson(msg)}","failedIndex":$idx}"""
  }

  private def parseObject(json: String): Map[String, String] = {
    val result = scala.collection.mutable.Map[String, String]()
    var depth  = 0
    var inKey  = false
    var inVal  = false
    var inStr  = false
    var escape = false
    val key    = new StringBuilder
    val value  = new StringBuilder

    for (c <- json.trim) {
      if (escape) {
        if (inKey) key.append(c)
        else if (inVal) value.append(c)
        escape = false
      } else {
        c match {
          case '\\' =>
            escape = true
            if (inKey) key.append(c)
            else if (inVal) value.append(c)
          case '"' if depth == 1 && !inKey && !inVal =>
            inKey = true
          case '"' if depth == 1 && inKey && !inStr =>
            inKey = false
          case '"' if depth == 1 && inVal && !inStr =>
            inStr = true
            value.append(c)
          case '"' if inStr =>
            value.append(c)
            inStr = false
          case ':' if depth == 1 && !inKey && !inVal =>
            inVal = true
          case ',' if depth == 1 && !inStr =>
            result(key.toString()) = cleanValue(value.toString())
            key.clear()
            value.clear()
            inVal = false
          case '{' | '[' =>
            depth += 1
            if (inVal) value.append(c)
          case '}' | ']' =>
            if (inVal && depth > 1) value.append(c)
            depth -= 1
            if (depth == 0 && key.nonEmpty) {
              result(key.toString()) = cleanValue(value.toString())
            }
          case _ =>
            if (inKey) key.append(c)
            else if (inVal) value.append(c)
        }
      }
    }
    result.toMap
  }

  private def cleanValue(v: String): String = {
    val trimmed = v.trim
    if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
      unescapeJson(trimmed.drop(1).dropRight(1))
    } else {
      trimmed
    }
  }

  private def parseComponentList(json: String): List[ComponentCheckpoint] = {
    if (json == "[]") return List.empty

    val components = scala.collection.mutable.ListBuffer[ComponentCheckpoint]()
    var depth      = 0
    val current    = new StringBuilder

    for (c <- json) {
      c match {
        case '[' if depth == 0 => depth += 1
        case ']' if depth == 1 => depth -= 1
        case '{' =>
          depth += 1
          current.append(c)
        case '}' =>
          current.append(c)
          depth -= 1
          if (depth == 1) {
            components += parseComponent(current.toString())
            current.clear()
          }
        case ',' if depth == 1 => // skip commas between objects
        case _ if depth > 1    => current.append(c)
        case _                 => // skip
      }
    }
    components.toList
  }

  private def parseComponent(json: String): ComponentCheckpoint = {
    val fields = parseObject(json)
    ComponentCheckpoint(
      index = fields("index").toInt,
      name = fields("name"),
      instanceType = fields("instanceType"),
      startedAt = Instant.parse(fields("startedAt")),
      completedAt = Instant.parse(fields("completedAt")),
      durationMs = fields("durationMs").toLong
    )
  }

  private def parseStatus(json: String): CheckpointStatus = {
    val fields = parseObject(json)
    fields("type") match {
      case "Running"   => CheckpointStatus.Running
      case "Completed" => CheckpointStatus.Completed
      case "Failed" =>
        CheckpointStatus.Failed(
          fields("errorMessage"),
          fields("failedIndex").toInt
        )
    }
  }
}
