package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._

final case class JmsConnectionConfig(
    queueName: String,
    username: Option[String],
    password: Option[String],
    messageSelector: Option[String],
    factoryProvider: String,
    brokerOptions: CaseInsensitiveConfigMap,
)

//noinspection ScalaWeakerAccess
object JmsConnectionConfig {

  val OptionQueueName = "jms.connection.queueName"
  val OptionUsername = "jms.connection.username"
  val OptionPassword = "jms.connection.password"
  val OptionMessageSelector = "jms.connection.messageSelector"
  val OptionFactoryProvider = "jms.connection.factoryProvider"

  private val BrokerOptionPrefix = "jms.connection.broker."

  def fromOptions(options: CaseInsensitiveConfigMap): JmsConnectionConfig = {
    JmsConnectionConfig(
      options.getRequired[String](OptionQueueName),
      options.getOptional[String](OptionUsername),
      options.getOptional[String](OptionPassword),
      options.getOptional[String](OptionMessageSelector),
      options.getRequired[String](OptionFactoryProvider),
      options.withPrefix(BrokerOptionPrefix),
    )
  }
}
