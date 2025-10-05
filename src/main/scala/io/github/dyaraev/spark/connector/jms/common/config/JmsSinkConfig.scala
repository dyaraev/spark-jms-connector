package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._

case class JmsSinkConfig(
    connection: JmsConnectionConfig,
    messageFormat: MessageFormat,
    throttlingDelayMs: Option[Int],
)

object JmsSinkConfig {

  val OptionMessageFormat = "jms.sink.messageFormat"
  val OptionNumPartitions = "jms.sink.throttlingDelayMs"

  def fromOptions(options: CaseInsensitiveConfigMap, connection: Option[JmsConnectionConfig] = None): JmsSinkConfig = {
    JmsSinkConfig(
      connection.getOrElse(JmsConnectionConfig.fromOptions(options)),
      options.getRequired[MessageFormat](OptionMessageFormat),
      options.getOptional[Int](OptionNumPartitions),
    )
  }
}
