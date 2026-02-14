package io.github.dyaraev.spark.connector.jms.common.client

import io.github.dyaraev.spark.connector.jms.common.config.JmsConnectionConfig
import io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils
import jakarta.jms._
import org.apache.spark.internal.Logging

import java.io.Closeable

/**
 * Lightweight JMS source client wrapping a single connection/session/consumer trio.
 *
 * @param connection
 *   JMS connection managed by this client.
 * @param session
 *   JMS session used to receive messages.
 * @param consumer
 *   JMS message consumer bound to the destination queue.
 */
class JmsSourceClient(connection: Connection, session: Session, consumer: MessageConsumer)
    extends Closeable
    with Logging {

  override def close(): Unit = {
    logInfo("Closing JMS connection")
    connection.close()
  }

  /**
   * Close the JMS connection, swallowing any exceptions.
   */
  def closeSilently(): Unit = {
    try close()
    catch CommonUtils.logException("Error while closing the connection")
  }

  /**
   * Receive a message, waiting up to the provided timeout.
   *
   * @param timeout
   *   Receive timeout in milliseconds.
   */
  def receive(timeout: Long): Message = consumer.receive(timeout)

  /**
   * Commit the current JMS session transaction.
   */
  def commit(): Unit = session.commit()
}

object JmsSourceClient extends Logging {

  /**
   * Create a JMS source client using connector connection settings.
   *
   * @param config
   *   Connection configuration including provider name, queue, and broker options.
   * @param transacted
   *   Whether to create a transacted session.
   */
  def apply(config: JmsConnectionConfig, transacted: Boolean): JmsSourceClient = {
    val factory = ConnectionFactoryCache.getOrCreate(config)
    JmsSourceClient(factory, config.queue, config.selector, config.username, config.password, transacted)
  }

  /**
   * Create a JMS source client from a connection factory.
   *
   * @param factory
   *   JMS connection factory to build the connection.
   * @param queue
   *   Destination queue name.
   * @param selector
   *   Optional JMS selector expression.
   * @param username
   *   Optional username for broker authentication.
   * @param password
   *   Optional password for broker authentication.
   * @param transacted
   *   Whether to create a transacted session.
   */
  def apply(
      factory: ConnectionFactory,
      queue: String,
      selector: Option[String] = None,
      username: Option[String] = None,
      password: Option[String] = None,
      transacted: Boolean = true,
  ): JmsSourceClient = {
    val sessionMode = if (transacted) Session.SESSION_TRANSACTED else Session.AUTO_ACKNOWLEDGE
    val connection = createConnection(factory, username, password)
    val session = connection.createSession(sessionMode)
    val consumer = createConsumer(session, queue, selector)
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

  private def createConsumer(session: Session, queue: String, selector: Option[String]): MessageConsumer = {
    val destination = session.createQueue(queue)
    session.createConsumer(destination, selector.orNull)
  }
}
