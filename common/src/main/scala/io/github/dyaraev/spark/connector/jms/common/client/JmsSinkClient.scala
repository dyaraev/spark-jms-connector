package io.github.dyaraev.spark.connector.jms.common.client

import io.github.dyaraev.spark.connector.jms.common.config.JmsConnectionConfig
import io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils
import jakarta.jms._
import org.apache.spark.internal.Logging

import java.io.Closeable

/**
 * Lightweight JMS sink client wrapping a single connection/session/producer trio.
 *
 * @param connection
 *   JMS connection managed by this client.
 * @param session
 *   JMS session used to create and send messages.
 * @param producer
 *   JMS message producer bound to the destination queue.
 */
class JmsSinkClient(connection: Connection, session: Session, producer: MessageProducer)
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
    catch CommonUtils.logException("Unexpected error while stopping the JMS connection")
  }

  /**
   * Send a bytes message to the configured destination.
   *
   * @param bytes
   *   Payload bytes to write to the message body.
   */
  def sendBytesMessage(bytes: Array[Byte]): Unit = {
    val message = session.createBytesMessage()
    message.writeBytes(bytes)
    producer.send(message)
  }

  /**
   * Send a text message to the configured destination.
   *
   * @param text
   *   Text payload to send.
   */
  def sendTextMessage(text: String): Unit = {
    val message = session.createTextMessage(text)
    producer.send(message)
  }

  /**
   * Commit the current JMS session transaction.
   */
  def commit(): Unit = session.commit()

  /**
   * Roll back the current JMS session transaction.
   */
  def rollback(): Unit = session.rollback()
}

object JmsSinkClient extends Logging {

  /**
   * Create a JMS sink client using connector connection settings.
   *
   * @param config
   *   Connection configuration including provider name, queue, and broker options.
   * @param transacted
   *   Whether to create a transacted session.
   */
  def apply(config: JmsConnectionConfig, transacted: Boolean): JmsSinkClient = {
    val factory = ConnectionFactoryCache.getOrCreate(config)
    JmsSinkClient(factory, config.queue, config.username, config.password, transacted)
  }

  /**
   * Create a JMS sink client from a connection factory.
   *
   * @param factory
   *   JMS connection factory to build the connection.
   * @param queue
   *   Destination queue name.
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
      username: Option[String] = None,
      password: Option[String] = None,
      transacted: Boolean = true,
  ): JmsSinkClient = {
    val sessionMode = if (transacted) Session.SESSION_TRANSACTED else Session.AUTO_ACKNOWLEDGE
    val connection = createConnection(factory, username, password)
    val session = connection.createSession(sessionMode)
    val destination = session.createQueue(queue)
    val producer = session.createProducer(destination)
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
