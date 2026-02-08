package io.github.dyaraev.spark.connector.jms.provider.activemq

import io.github.dyaraev.spark.connector.jms.common.ConnectionFactoryProvider
import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap
import jakarta.jms.ConnectionFactory
import org.apache.activemq.ActiveMQConnectionFactory

class ActiveMqConnectionFactoryProvider extends ConnectionFactoryProvider {

  override val brokerName: String = ActiveMqConnectionFactoryProvider.BrokerName

  override def getConnectionFactory(options: CaseInsensitiveConfigMap): ConnectionFactory = {
    val config = ActiveMqConfig.fromOptions(options)
    new ActiveMQConnectionFactory(config.url)
  }
}

object ActiveMqConnectionFactoryProvider {

  // noinspection ScalaWeakerAccess
  val BrokerName = "active-mq"
}
