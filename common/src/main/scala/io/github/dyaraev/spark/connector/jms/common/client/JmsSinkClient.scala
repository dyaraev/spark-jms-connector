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

  override def close(): Unit = {
    logInfo("Closing JMS connection")
    connection.close()
  }

  def closeSilently(): Unit = {
    try close()
    catch CommonUtils.logException("Unexpected error while stopping the JMS connection")
  }

  def sendBytesMessage(bytes: Array[Byte]): Unit = {
    val message = session.createBytesMessage()
    message.writeBytes(bytes)
    producer.send(message)
  }

  def sendTextMessage(text: String): Unit = {
    val message = session.createTextMessage(text)
    producer.send(message)
  }

  def commit(): Unit = session.commit()

  def rollback(): Unit = session.rollback()
}

object JmsSinkClient extends Logging {

  def apply(
      config: JmsConnectionConfig,
      transacted: Boolean,
  ): JmsSinkClient = {
    val provider = ConnectionFactoryProvider.createInstanceByBrokerName(config.brokerName)
    val factory = provider.getConnectionFactory(config.brokerOptions)
    JmsSinkClient(factory, config.queueName, config.username, config.password, transacted)
  }

  def apply(
      factory: ConnectionFactory,
      queueName: String,
      username: Option[String] = None,
      password: Option[String] = None,
      transacted: Boolean = true,
  ): JmsSinkClient = {
    val sessionMode = if (transacted) Session.SESSION_TRANSACTED else Session.AUTO_ACKNOWLEDGE
    val connection = createConnection(factory, username, password)
    val session = connection.createSession(sessionMode)
    val queue = session.createQueue(queueName)
    val producer = session.createProducer(queue)
    new JmsSinkClient(connection, session, producer)
  }

  private def createConnection(
      factory: ConnectionFactory,
      username: Option[String],
      password: Option[String],
  ): Connection = {
    logInfo(s"Creating JMS connection")
    val connection = username match {
      case Some(username) => factory.createConnection(username, password.orNull)
      case None           => factory.createConnection()
    }
    connection.start()
    connection
  }
}
