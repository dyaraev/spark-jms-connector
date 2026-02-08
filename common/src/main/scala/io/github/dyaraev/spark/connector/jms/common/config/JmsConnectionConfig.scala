package io.github.dyaraev.spark.connector.jms.common.config

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._

final case class JmsConnectionConfig(
    provider: String,
    queue: String,
    username: Option[String],
    password: Option[String],
    selector: Option[String],
    brokerOptions: CaseInsensitiveConfigMap,
)

object JmsConnectionConfig {

  // noinspection ScalaWeakerAccess
  val OptionProvider = "jms.connection.provider"

  // noinspection ScalaWeakerAccess
  val OptionQueue = "jms.connection.queue"

  // noinspection ScalaWeakerAccess
  val OptionUsername = "jms.connection.username"

  // noinspection ScalaWeakerAccess
  val OptionPassword = "jms.connection.password"

  // noinspection ScalaWeakerAccess
  val OptionSelector = "jms.connection.selector"

  private val BrokerOptionPrefix = "jms.connection.broker."

  def fromOptions(options: CaseInsensitiveConfigMap): JmsConnectionConfig = {
    JmsConnectionConfig(
      options.getRequired[String](OptionProvider),
      options.getRequired[String](OptionQueue),
      options.getOptional[String](OptionUsername),
      options.getOptional[String](OptionPassword),
      options.getOptional[String](OptionSelector),
      options.withPrefix(BrokerOptionPrefix),
    )
  }
}
