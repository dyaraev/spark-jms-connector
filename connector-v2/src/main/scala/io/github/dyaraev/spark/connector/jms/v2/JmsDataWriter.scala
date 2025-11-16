package io.github.dyaraev.spark.connector.jms.v2

import io.github.dyaraev.spark.connector.jms.common.client.JmsSinkClient
import io.github.dyaraev.spark.connector.jms.common.config.JmsSinkConfig
import io.github.dyaraev.spark.connector.jms.v2.JmsDataWriter.JmsCommitMessage
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

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
    commitJmsTransaction()
    message
  }

  override def close(): Unit = {
    closeClientIfExists()
  }

  override def abort(): Unit = {
    rollbackJmsTransaction()
    closeClientIfExists()
  }

  protected def sendMessage(f: JmsSinkClient => Unit): Unit = sendMessage(f, attempt = 1)

  @tailrec
  private def sendMessage(f: JmsSinkClient => Unit, attempt: Int): Unit = {
    Try(f(getOrCreateClient())) match {
      case Success(_)           =>
      case Failure(NonFatal(e)) =>
        if (attempt <= JmsDataWriter.MaxSendAttempts) {
          logError(s"Error sending message (attempt=$attempt), retrying ...", e)
          Thread.sleep((scala.math.pow(2, attempt.toDouble) * JmsDataWriter.MinRetryInterval).toLong)
          closeClientIfExists()
          sendMessage(f, attempt + 1)
        } else {
          throw new RuntimeException("Unable to send message", e)
        }
      case Failure(e) => throw e
    }
  }

  private def commitJmsTransaction(): Unit = {
    if (client != null && counter > 0) {
      logInfo(s"Committing $counter JMS messages")
      try {
        client.commit()
        counter = 0
      } catch {
        case NonFatal(e) =>
          logError(s"Failed to commit JMS transaction", e)
          throw new RuntimeException("JMS commit failed", e)
      }
    }
  }

  private def rollbackJmsTransaction(): Unit = {
    if (client != null && counter > 0) {
      logWarning("Aborting JMS transaction")
      try {
        client.rollback()
      } catch {
        case NonFatal(e) => logError("Error rolling back JMS transaction", e)
      }
    }
  }

  private def getOrCreateClient(): JmsSinkClient = {
    if (client == null) {
      client = JmsSinkClient(config.connection, transacted = true)
    }
    client
  }

  private def closeClientIfExists(): Unit = {
    if (client != null) {
      client.closeSilently()
      client = null
    }
  }
}

object JmsDataWriter {

  private val MaxSendAttempts: Int = 3

  private val MinRetryInterval: Long = 5000

  case class JmsCommitMessage(numMessages: Int) extends WriterCommitMessage
}
