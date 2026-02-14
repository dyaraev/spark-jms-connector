package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CaseInsensitiveConfigMapSpec extends AnyFunSuite with Matchers {

  test("getRequired should resolve keys case-insensitively") {
    val config = CaseInsensitiveConfigMap(Map("Some.Key" -> "value"))
    config.getRequired[String]("some.key") shouldBe "value"
  }

  test("getOptional should return None for missing keys") {
    val config = CaseInsensitiveConfigMap(Map("present" -> "1"))
    config.getOptional[String]("missing") shouldBe None
  }

  test("withPrefix should filter keys by prefix case-insensitively") {
    val config = CaseInsensitiveConfigMap(Map("JMS.Connection.A" -> "1", "other" -> "2"))
    val prefixed = config.withPrefix("jms.connection.")
    prefixed.getRequired[String]("JMS.Connection.A") shouldBe "1"
    prefixed.getOptional[String]("other") shouldBe None
  }

  test("equals and hashCode should use case-insensitive keys") {
    val left = CaseInsensitiveConfigMap(Map("Key" -> "value"))
    val right = CaseInsensitiveConfigMap(Map("kEy" -> "value"))
    left shouldBe right
    left.hashCode() shouldBe right.hashCode()
  }

  test("equals should not match different values") {
    val left = CaseInsensitiveConfigMap(Map("Key" -> "value"))
    val right = CaseInsensitiveConfigMap(Map("key" -> "other"))
    left should not be right
  }
}
