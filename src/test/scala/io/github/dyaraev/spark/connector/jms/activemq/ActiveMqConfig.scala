package io.github.dyaraev.spark.connector.jms.activemq

import io.github.dyaraev.spark.connector.jms.config.CaseInsensitiveConfigMap
import io.github.dyaraev.spark.connector.jms.config.CaseInsensitiveConfigMap.Implicits._

case class ActiveMqConfig(url: String)

object ActiveMqConfig {

  val OptionsJmsBrokerUrl = "jms.connection.broker.url"

  def fromOptions(options: CaseInsensitiveConfigMap): ActiveMqConfig = {
    ActiveMqConfig(options.getRequired[String](OptionsJmsBrokerUrl))
  }
}
