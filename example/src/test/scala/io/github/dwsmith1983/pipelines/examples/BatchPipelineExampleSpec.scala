package io.github.dwsmith1983.pipelines.examples

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Basic compile/run test for BatchPipelineExample.
 *
 * Verifies the canonical batch example compiles and key methods exist.
 */
class BatchPipelineExampleSpec extends AnyFunSuite with Matchers {

  test("BatchPipelineExample should compile and have main method") {
    // Verify object exists
    val example = BatchPipelineExample

    // Verify main method exists (will throw NoSuchMethodException if missing)
    val mainMethod = example.getClass.getMethod("main", classOf[Array[String]])
    mainMethod should not be null
  }
}
