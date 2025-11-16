package io.github.dyaraev.spark.connector.jms.common.client

import io.github.dyaraev.spark.connector.jms.common.ConnectionFactoryProvider
import io.github.dyaraev.spark.connector.jms.common.config.JmsConnectionConfig
import io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils
import jakarta.jms._
import org.apache.spark.internal.Logging

import java.io.Closeable

class JmsSourceClient(connection: Connection, session: Session, consumer: MessageConsumer)
    extends Closeable
    with Logging {

  override def close(): Unit = {
    logInfo("Closing JMS connection")
    connection.close()
  }

  def closeSilently(): Unit = {
    try close()
    catch CommonUtils.logException("Error while closing the connection")
  }

  def receiveNoWait: Message = consumer.receiveNoWait()

  def receive(timeout: Long): Message = consumer.receive(timeout)

  def commit(): Unit = session.commit()
}

object JmsSourceClient extends Logging {

  def apply(
      provider: ConnectionFactoryProvider,
      config: JmsConnectionConfig,
      transacted: Boolean,
  ): JmsSourceClient = {
    val factory = provider.getConnectionFactory(config.brokerOptions)
    JmsSourceClient(factory, config.queueName, config.messageSelector, config.username, config.password, transacted)
  }

  def apply(
      factory: ConnectionFactory,
      queueName: String,
      messageSelector: Option[String] = None,
      username: Option[String] = None,
      password: Option[String] = None,
      transacted: Boolean = true,
  ): JmsSourceClient = {
    val sessionMode = if (transacted) Session.SESSION_TRANSACTED else Session.AUTO_ACKNOWLEDGE
    val connection = createConnection(factory, username, password)
    val session = connection.createSession(sessionMode)
    val consumer = createConsumer(session, queueName, messageSelector)
    new JmsSourceClient(connection, session, consumer)
  }

  private def createConnection(
      connectionFactory: ConnectionFactory,
      username: Option[String],
      password: Option[String],
  ): Connection = {
    logInfo(s"Creating JMS connection")
    val connection = username match {
      case Some(username) => connectionFactory.createConnection(username, password.orNull)
      case None           => connectionFactory.createConnection()
    }
    connection.start()
    connection
  }

  private def createConsumer(session: Session, queueName: String, selector: Option[String]): MessageConsumer = {
    val queue = session.createQueue(queueName)
    session.createConsumer(queue, selector.orNull)
  }
}
