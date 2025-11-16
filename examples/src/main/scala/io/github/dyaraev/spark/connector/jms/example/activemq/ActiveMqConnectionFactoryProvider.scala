package io.github.dyaraev.spark.connector.jms.example.activemq

import io.github.dyaraev.spark.connector.jms.common.ConnectionFactoryProvider
import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap
import jakarta.jms.ConnectionFactory
import org.apache.activemq.ActiveMQConnectionFactory

class ActiveMqConnectionFactoryProvider extends ConnectionFactoryProvider {

  override val brokerName: String = "active-mq"

  override def getConnectionFactory(options: CaseInsensitiveConfigMap): ConnectionFactory = {
    val config = ActiveMqConfig.fromOptions(options)
    new ActiveMQConnectionFactory(config.url)
  }
}
