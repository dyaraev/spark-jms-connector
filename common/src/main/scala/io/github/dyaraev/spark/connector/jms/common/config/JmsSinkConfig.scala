package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._

case class JmsSinkConfig(
    connection: JmsConnectionConfig,
    messageFormat: MessageFormat,
    throttlingDelayMs: Option[Int],
) {

  def validate(): Unit = {
    throttlingDelayMs.foreach(d => require(d > 0, "Throttling delay must be positive"))
  }
}

object JmsSinkConfig {

  val OptionMessageFormat = "jms.sink.messageFormat"
  val OptionThrottlingDelayMs = "jms.sink.throttlingDelayMs"

  def fromOptions(options: CaseInsensitiveConfigMap, connection: Option[JmsConnectionConfig] = None): JmsSinkConfig = {
    val config = JmsSinkConfig(
      connection.getOrElse(JmsConnectionConfig.fromOptions(options)),
      options.getRequired[MessageFormat](OptionMessageFormat),
      options.getOptional[Int](OptionThrottlingDelayMs),
    )
    config.validate()
    config
  }
}
