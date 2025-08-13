package io.github.dyaraev.spark.connector.jms


import io.github.dyaraev.spark.connector.jms.JmsSourceConfig.ActiveMqConfig
import jakarta.jms.ConnectionFactory
import org.apache.activemq.ActiveMQConnectionFactory

// TODO: make pluggable
object ConnectionFactoryProvider {

  def newConnectionFactory(sourceConf: JmsSourceConfig): ConnectionFactory = {
    sourceConf.broker match {
      case brokerConf: ActiveMqConfig => newActiveMqConnectionFactory(brokerConf)
      case another => throw new RuntimeException(s"Unsupported JMS implementation: ${another.getClass.getName}")
    }
  }

  private def newActiveMqConnectionFactory(brokerConf: ActiveMqConfig): ConnectionFactory = {
    new ActiveMQConnectionFactory(brokerConf.url)
  }
}