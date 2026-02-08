package io.github.dyaraev.spark.connector.jms.v2

import io.github.dyaraev.spark.connector.jms.common.client.JmsSinkClient
import io.github.dyaraev.spark.connector.jms.common.config.JmsSinkConfig
import io.github.dyaraev.spark.connector.jms.v2.JmsDataWriter.JmsCommitMessage
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

import scala.util.control.NonFatal

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
    val currentCounter = counter
    commitJmsTransaction()
    JmsCommitMessage(currentCounter)
  }

  override def close(): Unit = {
    closeClientIfExists()
  }

  override def abort(): Unit = {
    rollbackJmsTransaction()
    closeClientIfExists()
  }

  protected def sendMessage(f: JmsSinkClient => Unit): Unit = {
    try {
      f(getOrCreateClient())
    } catch {
      case NonFatal(e) =>
        rollbackJmsTransaction()
        closeClientIfExists()
        throw new RuntimeException("Failed to send JMS message ", e)
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
          closeClientIfExists()
          throw new RuntimeException("Failed to commit JMS transaction", e)
      }
    }
  }

  private def rollbackJmsTransaction(): Unit = {
    if (client != null) {
      logWarning("Aborting JMS transaction")
      try client.rollback()
      catch {
        case NonFatal(e) =>
          logError("Failed to rollback JMS transaction", e)
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

  case class JmsCommitMessage(numMessages: Int) extends WriterCommitMessage
}
