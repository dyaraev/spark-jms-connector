package io.github.dyaraev.spark.connector.jms.activemq

import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class ActiveMqConfig(url: String)

object ActiveMqConfig {

  private val OptionsJmsBrokerUrl = "jms.broker.url"

  def fromOptions(options: CaseInsensitiveStringMap): ActiveMqConfig = {
    ActiveMqConfig(options.get(OptionsJmsBrokerUrl))
  }
}
