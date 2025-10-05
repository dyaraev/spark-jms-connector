package io.github.dyaraev.spark.connector.jms.common.client

import io.github.dyaraev.spark.connector.jms.common.ConnectionFactoryProvider
import io.github.dyaraev.spark.connector.jms.common.config.JmsConnectionConfig
import io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils
import jakarta.jms._
import org.apache.spark.internal.Logging

class JmsSourceClient(connection: Connection, consumer: MessageConsumer) extends Logging {

  def receiveNoWait: Message = consumer.receiveNoWait()

  def receive(timeout: Long): Message = consumer.receive(timeout)

  def closeSilently(): Unit = {
    try connection.close()
    catch CommonUtils.logException("Error while closing the connection")
  }
}

object JmsSourceClient {

  def apply(provider: ConnectionFactoryProvider, config: JmsConnectionConfig): JmsSourceClient = {
    val factory = provider.getConnectionFactory(config.brokerOptions)
    val connection = createConnection(factory, config.username, config.password)
    val session = connection.createSession(false, JMSContext.CLIENT_ACKNOWLEDGE)
    val consumer = createConsumer(session, config.queueName, config.messageSelector)
    new JmsSourceClient(connection, consumer)
  }

  private def createConnection(
      connectionFactory: ConnectionFactory,
      username: Option[String],
      password: Option[String],
  ): Connection = {
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
