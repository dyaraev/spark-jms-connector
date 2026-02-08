package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._

case class JmsSinkConfig(
    connection: JmsConnectionConfig,
    messageFormat: MessageFormat,
    throttlingDelayMs: Option[Long],
) {

  def validate(): Unit = {
    throttlingDelayMs.foreach(d => require(d > 0, "Throttling delay must be positive"))
  }
}

object JmsSinkConfig {

  // noinspection ScalaWeakerAccess
  val OptionMessageFormat = "jms.messageFormat"

  // noinspection ScalaWeakerAccess
  val OptionThrottlingDelayMs = "jms.throttlingDelayMs"

  def fromOptions(options: CaseInsensitiveConfigMap, connection: Option[JmsConnectionConfig] = None): JmsSinkConfig = {
    val config = JmsSinkConfig(
      connection.getOrElse(JmsConnectionConfig.fromOptions(options)),
      options.getRequired[MessageFormat](OptionMessageFormat),
      options.getOptional[Long](OptionThrottlingDelayMs),
    )
    config.validate()
    config
  }
}
