package io.github.dwsmith1983.spark.pipeline.config

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

/**
 * Comprehensive tests for ComponentInstantiator.
 *
 * Tests cover:
 * - Happy path: successful instantiation of valid components
 * - Unhappy path: various failure scenarios
 * - Edge cases: unusual but valid scenarios
 */
class ComponentInstantiatorSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {

  describe("ComponentInstantiator") {

    // =========================================================================
    // HAPPY PATH TESTS
    // =========================================================================

    describe("happy path") {

      it("should instantiate a component with valid config") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponent",
          instanceName = "TestComponent(happy)",
          instanceConfig = ConfigFactory.parseString("""
            name = "test"
            value = 42
          """)
        )

        val component = ComponentInstantiator.instantiate[TestComponent](config)

        component shouldBe a[TestComponent]
        component.conf.name shouldBe "test"
        component.conf.value shouldBe 42
      }

      it("should instantiate a component with default config values") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponentWithDefaults",
          instanceName = "TestComponentWithDefaults(happy)",
          instanceConfig = ConfigFactory.parseString("""
            required-field = "provided"
          """)
        )

        val component = ComponentInstantiator.instantiate[TestComponentWithDefaults](config)

        component shouldBe a[TestComponentWithDefaults]
        component.conf.requiredField shouldBe "provided"
        component.conf.optionalField shouldBe "default"
        component.conf.optionalInt shouldBe 100
      }

      it("should instantiate a component with nested config") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponentWithNestedConfig",
          instanceName = "Nested(happy)",
          instanceConfig = ConfigFactory.parseString("""
            outer-name = "outer"
            inner {
              inner-name = "inner"
              inner-value = 99
            }
          """)
        )

        val component = ComponentInstantiator.instantiate[TestComponentWithNestedConfig](config)

        component.conf.outerName shouldBe "outer"
        component.conf.inner.innerName shouldBe "inner"
        component.conf.inner.innerValue shouldBe 99
      }

      it("should instantiate a component with list config") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponentWithListConfig",
          instanceName = "List(happy)",
          instanceConfig = ConfigFactory.parseString("""
            items = [
              { name = "item1", value = 1 },
              { name = "item2", value = 2 },
              { name = "item3", value = 3 }
            ]
          """)
        )

        val component = ComponentInstantiator.instantiate[TestComponentWithListConfig](config)

        component.conf.items should have size 3
        component.conf.items.head.name shouldBe "item1"
        component.conf.items.last.value shouldBe 3
      }

      it("should return Success when using tryInstantiate with valid config") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponent",
          instanceName = "Test",
          instanceConfig = ConfigFactory.parseString("""
            name = "test"
            value = 1
          """)
        )

        val result = ComponentInstantiator.tryInstantiate[TestComponent](config)

        result.isSuccess shouldBe true
        result.get.conf.name shouldBe "test"
      }
    }

    // =========================================================================
    // UNHAPPY PATH TESTS
    // =========================================================================

    describe("unhappy path") {

      it("should throw ComponentInstantiationException for non-existent class") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.NonExistentClass",
          instanceName = "NonExistent",
          instanceConfig = ConfigFactory.empty()
        )

        val exception = intercept[ComponentInstantiationException] {
          ComponentInstantiator.instantiate[Any](config)
        }

        exception.getMessage should include("Could not find class")
        exception.getMessage should include("io.github.dwsmith1983.NonExistentClass")
      }

      it("should throw ComponentInstantiationException for class without companion object") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.ClassWithoutCompanion",
          instanceName = "NoCompanion",
          instanceConfig = ConfigFactory.empty()
        )

        val exception = intercept[ComponentInstantiationException] {
          ComponentInstantiator.instantiate[Any](config)
        }

        exception.getMessage should include("Could not find companion object")
      }

      it("should throw ComponentInstantiationException for companion not extending ConfigurableInstance") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.ClassWithNonConfigurableCompanion",
          instanceName = "NotConfigurable",
          instanceConfig = ConfigFactory.empty()
        )

        val exception = intercept[ComponentInstantiationException] {
          ComponentInstantiator.instantiate[Any](config)
        }

        exception.getMessage should include("does not extend ConfigurableInstance")
      }

      it("should throw ConfigurationException for missing required config fields") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponent",
          instanceName = "MissingFields",
          instanceConfig = ConfigFactory.parseString("""
            name = "test"
            # missing 'value' field
          """)
        )

        val exception = intercept[ConfigurationException] {
          ComponentInstantiator.instantiate[TestComponent](config)
        }

        exception.getMessage should include("value")
      }

      it("should throw ConfigurationException for wrong config type") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponent",
          instanceName = "WrongType",
          instanceConfig = ConfigFactory.parseString("""
            name = "test"
            value = "not-an-int"
          """)
        )

        val exception = intercept[ConfigurationException] {
          ComponentInstantiator.instantiate[TestComponent](config)
        }

        exception.getMessage should include("value")
      }

      it("should return Failure when using tryInstantiate with invalid config") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.NonExistent",
          instanceName = "Test",
          instanceConfig = ConfigFactory.empty()
        )

        val result = ComponentInstantiator.tryInstantiate[Any](config)

        result.isFailure shouldBe true
        result.failed.get shouldBe a[ComponentInstantiationException]
      }

      it("should propagate exception from createFromConfig") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponentThatThrows",
          instanceName = "Throws",
          instanceConfig = ConfigFactory.parseString("""
            should-throw = true
          """)
        )

        val exception = intercept[ComponentInstantiationException] {
          ComponentInstantiator.instantiate[Any](config)
        }

        exception.getMessage should include("Failed to instantiate component")
        exception.getCause shouldBe a[IllegalArgumentException]
      }
    }

    // =========================================================================
    // EDGE CASE TESTS
    // =========================================================================

    describe("edge cases") {

      it("should handle empty config when component allows it") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponentAllowingEmptyConfig",
          instanceName = "Empty",
          instanceConfig = ConfigFactory.empty()
        )

        val component = ComponentInstantiator.instantiate[TestComponentAllowingEmptyConfig](config)

        component shouldBe a[TestComponentAllowingEmptyConfig]
      }

      it("should handle config with extra unused fields") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponent",
          instanceName = "ExtraFields",
          instanceConfig = ConfigFactory.parseString("""
            name = "test"
            value = 42
            extra-field = "ignored"
            another-extra = 999
          """)
        )

        val component = ComponentInstantiator.instantiate[TestComponent](config)

        component.conf.name shouldBe "test"
        component.conf.value shouldBe 42
      }

      it("should handle deeply nested class names") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.outer.inner.deeply.nested.DeepComponent",
          instanceName = "Deep",
          instanceConfig = ConfigFactory.parseString("""
            depth = 5
          """)
        )

        val component = ComponentInstantiator.instantiate[outer.inner.deeply.nested.DeepComponent](config)

        component.conf.depth shouldBe 5
      }

      it("should handle special characters in config values") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponentWithStringConfig",
          instanceName = "SpecialChars",
          instanceConfig = ConfigFactory.parseString("""
            text = "Hello \"World\" with\nnewlines\tand\ttabs"
            path = "/data/path with spaces/file.txt"
          """)
        )

        val component = ComponentInstantiator.instantiate[TestComponentWithStringConfig](config)

        component.conf.text should include("\"World\"")
        component.conf.text should include("\n")
        component.conf.path should include("path with spaces")
      }

      it("should handle unicode in config values") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponentWithStringConfig",
          instanceName = "Unicode",
          instanceConfig = ConfigFactory.parseString("""
            text = "日本語テスト émojis 🚀"
            path = "/データ/パス"
          """)
        )

        val component = ComponentInstantiator.instantiate[TestComponentWithStringConfig](config)

        component.conf.text should include("日本語")
        component.conf.text should include("🚀")
      }

      it("should handle very large numeric values") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponentWithLargeNumbers",
          instanceName = "LargeNumbers",
          instanceConfig = ConfigFactory.parseString("""
            big-long = 9223372036854775807
            big-double = 1.7976931348623157E308
          """)
        )

        val component = ComponentInstantiator.instantiate[TestComponentWithLargeNumbers](config)

        component.conf.bigLong shouldBe Long.MaxValue
        component.conf.bigDouble shouldBe Double.MaxValue
      }

      it("should handle empty list in config") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponentWithListConfig",
          instanceName = "EmptyList",
          instanceConfig = ConfigFactory.parseString("""
            items = []
          """)
        )

        val component = ComponentInstantiator.instantiate[TestComponentWithListConfig](config)

        component.conf.items shouldBe empty
      }

      it("should handle optional fields set to null") {
        val config = ComponentConfig(
          instanceType = "io.github.dwsmith1983.spark.pipeline.config.TestComponentWithOptionConfig",
          instanceName = "NullOption",
          instanceConfig = ConfigFactory.parseString("""
            required = "present"
            # optional is not set
          """)
        )

        val component = ComponentInstantiator.instantiate[TestComponentWithOptionConfig](config)

        component.conf.required shouldBe "present"
        component.conf.optional shouldBe None
      }
    }
  }
}

// =============================================================================
// TEST FIXTURES - Components used for testing
// =============================================================================

// Basic test component
case class TestConfig(name: String, value: Int)

object TestComponent extends ConfigurableInstance {

  import pureconfig._
  import pureconfig.generic.auto._

  override def createFromConfig(conf: Config): TestComponent =
    new TestComponent(ConfigSource.fromConfig(conf).loadOrThrow[TestConfig])
}

class TestComponent(val conf: TestConfig) extends PipelineComponent {
  override def run(): Unit = ()
}

// Component with default values
case class TestConfigWithDefaults(
  requiredField: String,
  optionalField: String = "default",
  optionalInt: Int = 100)

object TestComponentWithDefaults extends ConfigurableInstance {

  import pureconfig._
  import pureconfig.generic.auto._

  override def createFromConfig(conf: Config): TestComponentWithDefaults =
    new TestComponentWithDefaults(ConfigSource.fromConfig(conf).loadOrThrow[TestConfigWithDefaults])
}

class TestComponentWithDefaults(val conf: TestConfigWithDefaults) extends PipelineComponent {
  override def run(): Unit = ()
}

// Component with nested config
case class InnerConfig(innerName: String, innerValue: Int)
case class NestedConfig(outerName: String, inner: InnerConfig)

object TestComponentWithNestedConfig extends ConfigurableInstance {

  import pureconfig._
  import pureconfig.generic.auto._

  override def createFromConfig(conf: Config): TestComponentWithNestedConfig =
    new TestComponentWithNestedConfig(ConfigSource.fromConfig(conf).loadOrThrow[NestedConfig])
}

class TestComponentWithNestedConfig(val conf: NestedConfig) extends PipelineComponent {
  override def run(): Unit = ()
}

// Component with list config
case class ItemConfig(name: String, value: Int)
case class ListConfig(items: List[ItemConfig])

object TestComponentWithListConfig extends ConfigurableInstance {

  import pureconfig._
  import pureconfig.generic.auto._

  override def createFromConfig(conf: Config): TestComponentWithListConfig =
    new TestComponentWithListConfig(ConfigSource.fromConfig(conf).loadOrThrow[ListConfig])
}

class TestComponentWithListConfig(val conf: ListConfig) extends PipelineComponent {
  override def run(): Unit = ()
}

// Component that throws on creation
object TestComponentThatThrows extends ConfigurableInstance {

  override def createFromConfig(conf: Config): Any = {
    if (conf.getBoolean("should-throw")) {
      throw new IllegalArgumentException("Intentional failure")
    }
    new Object()
  }
}

// Component allowing empty config
case class EmptyConfig()

object TestComponentAllowingEmptyConfig extends ConfigurableInstance {

  override def createFromConfig(conf: Config): TestComponentAllowingEmptyConfig =
    new TestComponentAllowingEmptyConfig()
}

class TestComponentAllowingEmptyConfig() extends PipelineComponent {
  override def run(): Unit = ()
}

// Class without companion object (for negative testing)
class ClassWithoutCompanion extends PipelineComponent {
  override def run(): Unit = ()
}

// Class with non-configurable companion
object ClassWithNonConfigurableCompanion {
  def create(): ClassWithNonConfigurableCompanion = new ClassWithNonConfigurableCompanion()
}

class ClassWithNonConfigurableCompanion extends PipelineComponent {
  override def run(): Unit = ()
}

// Component with string config (for special chars testing)
case class StringConfig(text: String, path: String)

object TestComponentWithStringConfig extends ConfigurableInstance {

  import pureconfig._
  import pureconfig.generic.auto._

  override def createFromConfig(conf: Config): TestComponentWithStringConfig =
    new TestComponentWithStringConfig(ConfigSource.fromConfig(conf).loadOrThrow[StringConfig])
}

class TestComponentWithStringConfig(val conf: StringConfig) extends PipelineComponent {
  override def run(): Unit = ()
}

// Component with large numbers
case class LargeNumberConfig(bigLong: Long, bigDouble: Double)

object TestComponentWithLargeNumbers extends ConfigurableInstance {

  import pureconfig._
  import pureconfig.generic.auto._

  override def createFromConfig(conf: Config): TestComponentWithLargeNumbers =
    new TestComponentWithLargeNumbers(ConfigSource.fromConfig(conf).loadOrThrow[LargeNumberConfig])
}

class TestComponentWithLargeNumbers(val conf: LargeNumberConfig) extends PipelineComponent {
  override def run(): Unit = ()
}

// Component with Option config
case class OptionConfig(required: String, optional: Option[String] = None)

object TestComponentWithOptionConfig extends ConfigurableInstance {

  import pureconfig._
  import pureconfig.generic.auto._

  override def createFromConfig(conf: Config): TestComponentWithOptionConfig =
    new TestComponentWithOptionConfig(ConfigSource.fromConfig(conf).loadOrThrow[OptionConfig])
}

class TestComponentWithOptionConfig(val conf: OptionConfig) extends PipelineComponent {
  override def run(): Unit = ()
}

// Deeply nested package component
package outer.inner.deeply.nested {
  case class DeepConfig(depth: Int)

  object DeepComponent extends ConfigurableInstance {

    import pureconfig._
    import pureconfig.generic.auto._

    override def createFromConfig(conf: Config): DeepComponent =
      new DeepComponent(ConfigSource.fromConfig(conf).loadOrThrow[DeepConfig])
  }

  class DeepComponent(val conf: DeepConfig) extends PipelineComponent {
    override def run(): Unit = ()
  }
}
