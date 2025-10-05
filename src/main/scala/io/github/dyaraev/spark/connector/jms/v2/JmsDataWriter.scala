package io.github.dyaraev.spark.connector.jms.v2

import io.github.dyaraev.spark.connector.jms.common.client.JmsSinkClient
import io.github.dyaraev.spark.connector.jms.common.config.JmsSinkConfig
import io.github.dyaraev.spark.connector.jms.v2.JmsDataWriter.JmsCommitMessage
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

import scala.annotation.tailrec

trait JmsDataWriter[T] extends DataWriter[InternalRow] with Serializable with Logging {

  @transient
  private var client: JmsSinkClient = _

  private var counter = 0

  def config: JmsSinkConfig

  def fromRow: InternalRow => T

  def writeValue(value: T): Unit

  override def write(row: InternalRow): Unit = {
    writeValue(fromRow(row))
    counter += 1
  }

  override def commit(): WriterCommitMessage = {
    val message = JmsCommitMessage(counter)
    counter = 0
    message
  }

  override def close(): Unit = closeClientIfExists()

  override def abort(): Unit = {}

  protected def sendMessage(f: JmsSinkClient => Unit): Unit = sendMessage(f, attempt = 1)

  @tailrec
  private def sendMessage(f: JmsSinkClient => Unit, attempt: Int): Unit = {
    try f(getOrCreateClient())
    catch {
      case e: Throwable =>
        if (attempt <= JmsDataWriter.MaxSendAttempts) {
          logError(s"Error sending message (attempt=$attempt), retrying ...", e)
          Thread.sleep(attempt * JmsDataWriter.MinRetryInterval)
          closeClientIfExists()
          sendMessage(f, attempt + 1)
        } else {
          throw new RuntimeException("Unable to send message", e)
        }
    }
  }

  private def getOrCreateClient(): JmsSinkClient = {
    if (client == null) {
      client = JmsSinkClient(config.connection, () => _)
    }
    client
  }

  private def closeClientIfExists(): Unit = {
    if (client != null) {
      client.close()
      client = null
    }
  }
}

object JmsDataWriter {

  private val MaxSendAttempts = 3

  private val MinRetryInterval = 5000

  case class JmsCommitMessage(numMessages: Int) extends WriterCommitMessage
}
