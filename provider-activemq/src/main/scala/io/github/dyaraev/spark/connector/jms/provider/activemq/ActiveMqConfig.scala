package io.github.dyaraev.spark.connector.jms.provider.activemq

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap
import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._

case class ActiveMqConfig(url: String)

object ActiveMqConfig {

  // noinspection ScalaWeakerAccess
  val OptionsJmsBrokerUrl = "jms.connection.broker.url"

  def fromOptions(options: CaseInsensitiveConfigMap): ActiveMqConfig = {
    ActiveMqConfig(options.getRequired[String](OptionsJmsBrokerUrl))
  }
}
