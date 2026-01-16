package io.github.dwsmith1983.pipelines.examples

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Basic compile/run test for StreamingPipelineExample.
 *
 * Verifies the canonical streaming example compiles and key methods exist.
 */
class StreamingPipelineExampleSpec extends AnyFunSuite with Matchers {

  test("StreamingPipelineExample should compile and have main method") {
    // Verify object exists
    val example = StreamingPipelineExample

    // Verify main method exists (will throw NoSuchMethodException if missing)
    val mainMethod = example.getClass.getMethod("main", classOf[Array[String]])
    mainMethod should not be null
  }

  test("example methods should be accessible") {
    // Verify the example methods exist
    val simpleMethod = StreamingPipelineExample.getClass.getMethod("runSimpleStreamingExample")
    val windowedMethod = StreamingPipelineExample.getClass.getMethod("runWindowedAggregationExample")
    val statefulMethod = StreamingPipelineExample.getClass.getMethod("runStatefulProcessingExample")

    simpleMethod should not be null
    windowedMethod should not be null
    statefulMethod should not be null
  }
}
