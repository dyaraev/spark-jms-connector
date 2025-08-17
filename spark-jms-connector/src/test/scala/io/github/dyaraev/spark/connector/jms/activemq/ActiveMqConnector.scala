package io.github.dyaraev.spark.connector.jms.activemq

import io.github.dyaraev.spark.connector.jms.JmsConnector
import jakarta.jms.ConnectionFactory
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ActiveMqConnector extends JmsConnector {

  override val brokerName: String = "active-mq"

  override def getConnectionFactory(options: CaseInsensitiveStringMap): ConnectionFactory = {
    val config = ActiveMqConfig.fromOptions(options)
    new ActiveMQConnectionFactory(config.url)
  }
}
