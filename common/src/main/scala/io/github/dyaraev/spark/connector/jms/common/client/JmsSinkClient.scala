package io.github.dyaraev.spark.connector.jms.common.client

import io.github.dyaraev.spark.connector.jms.common.ConnectionFactoryProvider
import io.github.dyaraev.spark.connector.jms.common.config.JmsConnectionConfig
import io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils
import jakarta.jms._
import org.apache.spark.internal.Logging

import java.io.Closeable

class JmsSinkClient(connection: Connection, session: Session, producer: MessageProducer)
    extends Closeable
    with Logging {

  def sendBytesMessage(bytes: Array[Byte]): Unit = {
    val message = session.createBytesMessage()
    message.writeBytes(bytes)
    producer.send(message)
  }

  def sendTextMessage(text: String): Unit = {
    val message = session.createTextMessage(text)
    producer.send(message)
  }

  def closeSilently(): Unit = {
    try connection.close()
    catch CommonUtils.logException("Unexpected error while stopping the JMS connection")
  }

  override def close(): Unit = connection.close()
}

object JmsSinkClient {

  def apply(config: JmsConnectionConfig, listener: Option[ExceptionListener]): JmsSinkClient = {
    val provider = ConnectionFactoryProvider.createInstance(config.factoryProvider)
    val factory = provider.getConnectionFactory(config.brokerOptions)
    val connection = config.username match {
      case Some(username) => factory.createConnection(username, config.password.orNull)
      case None           => factory.createConnection()
    }
    listener.foreach(connection.setExceptionListener)
    connection.start()

    val session = connection.createSession(false, JMSContext.CLIENT_ACKNOWLEDGE)
    val queue = session.createQueue(config.queueName)
    val producer = session.createProducer(queue)
    new JmsSinkClient(connection, session, producer)
  }

  def apply(
      factory: ConnectionFactory,
      queueName: String,
      username: Option[String] = None,
      password: Option[String] = None,
      listener: Option[ExceptionListener] = None,
  ): JmsSinkClient = {
    val connection = username match {
      case Some(username) => factory.createConnection(username, password.orNull)
      case None           => factory.createConnection()
    }
    listener.foreach(connection.setExceptionListener)
    connection.start()

    val session = connection.createSession(false, JMSContext.CLIENT_ACKNOWLEDGE)
    val queue = session.createQueue(queueName)
    val producer = session.createProducer(queue)
    new JmsSinkClient(connection, session, producer)
  }
}
