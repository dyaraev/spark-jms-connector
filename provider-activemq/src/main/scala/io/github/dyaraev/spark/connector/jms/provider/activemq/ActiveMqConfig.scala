package io.github.dyaraev.spark.connector.jms.provider.activemq

import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap
import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._

case class ActiveMqConfig(url: String)

object ActiveMqConfig {

  val BrokerName = "active-mq"

  val OptionsJmsBrokerAddress = "jms.connection.broker.address"

  def fromOptions(options: CaseInsensitiveConfigMap): ActiveMqConfig = {
    ActiveMqConfig(options.getRequired[String](OptionsJmsBrokerAddress))
  }
}
