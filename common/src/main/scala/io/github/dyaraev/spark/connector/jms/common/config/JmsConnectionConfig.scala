package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._

final case class JmsConnectionConfig(
    brokerName: String,
    queueName: String,
    username: Option[String],
    password: Option[String],
    messageSelector: Option[String],
    brokerOptions: CaseInsensitiveConfigMap,
)

object JmsConnectionConfig {

  // noinspection ScalaWeakerAccess
  val OptionQueueName = "jms.connection.queueName"

  // noinspection ScalaWeakerAccess
  val OptionUsername = "jms.connection.username"

  // noinspection ScalaWeakerAccess
  val OptionPassword = "jms.connection.password"

  // noinspection ScalaWeakerAccess
  val OptionMessageSelector = "jms.connection.messageSelector"

  // noinspection ScalaWeakerAccess
  val OptionBrokerName = "jms.connection.broker.name"

  private val BrokerOptionPrefix = "jms.connection.broker."

  def fromOptions(options: CaseInsensitiveConfigMap): JmsConnectionConfig = {
    JmsConnectionConfig(
      options.getRequired[String](OptionBrokerName),
      options.getRequired[String](OptionQueueName),
      options.getOptional[String](OptionUsername),
      options.getOptional[String](OptionPassword),
      options.getOptional[String](OptionMessageSelector),
      options.withPrefix(BrokerOptionPrefix),
    )
  }
}
