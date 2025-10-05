package io.github.dyaraev.spark.connector.jms.common.client

import io.github.dyaraev.spark.connector.jms.common.ConnectionFactoryProvider
import io.github.dyaraev.spark.connector.jms.common.config.JmsConnectionConfig
import io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils
import jakarta.jms._
import org.apache.spark.internal.Logging

class JmsSinkClient(connection: Connection, session: Session, producer: MessageProducer) extends Logging {

  def sendBytesMessage(bytes: Array[Byte]): Unit = {
    val message = session.createBytesMessage()
    message.writeBytes(bytes)
    producer.send(message)
  }

  def sendTextMessage(text: String): Unit = {
    val message = session.createTextMessage(text)
    producer.send(message)
  }

  def close(): Unit = {
    try connection.close()
    catch CommonUtils.logException("Unexpected error while stopping the JMS connection")
  }
}

object JmsSinkClient {

  def apply(config: JmsConnectionConfig, exceptionListener: ExceptionListener): JmsSinkClient = {
    val provider = ConnectionFactoryProvider.createInstance(config.factoryProvider)
    val factory = provider.getConnectionFactory(config.brokerOptions)
    val connection = config.username match {
      case Some(username) => factory.createConnection(username, config.password.orNull)
      case None           => factory.createConnection()
    }
    connection.setExceptionListener(exceptionListener)
    connection.start()

    val session = connection.createSession(false, JMSContext.CLIENT_ACKNOWLEDGE)
    val queue = session.createQueue(config.queueName)
    val producer = session.createProducer(queue)
    new JmsSinkClient(connection, session, producer)
  }
}
