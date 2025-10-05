package io.github.dyaraev.spark.connector.jms.common.metadata

import jakarta.jms.{BytesMessage, Message, TextMessage}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

sealed trait LogEntry {

  def toInternalRow: InternalRow
}

object LogEntry {

  case class BinaryLogEntry(id: String, sentTs: Long, receivedTs: Long, value: Array[Byte]) extends LogEntry {

    def this(message: BytesMessage) = {
      this(message.getJMSMessageID, message.getJMSTimestamp, System.currentTimeMillis(), fetchMessageBody(message))
    }

    def toInternalRow: InternalRow = {
      InternalRow(UTF8String.fromString(id), sentTs, receivedTs, value)
    }
  }

  case class TextLogEntry(id: String, sentTs: Long, receivedTs: Long, value: Array[Byte]) extends LogEntry {

    def this(message: TextMessage) = {
      this(message.getJMSMessageID, message.getJMSTimestamp, System.currentTimeMillis(), message.getText.getBytes)
    }

    def toInternalRow: InternalRow = {
      InternalRow(UTF8String.fromString(id), sentTs, receivedTs, UTF8String.fromBytes(value))
    }
  }

  def fromMessage[T <: LogEntry](message: Message)(implicit toEntry: Message => T): T = toEntry(message)

  object Implicits {

    implicit val toTextLogEntry: Message => TextLogEntry = {
      case tm: TextMessage => new TextLogEntry(tm)
      case m => throw new RuntimeException(s"Expected TextMessage but received ${m.getClass}")
    }

    implicit val toBinaryLogEntry: Message => BinaryLogEntry = {
      case bm: BytesMessage => new BinaryLogEntry(bm)
      case m => throw new RuntimeException(s"Expected BytesMessage but received ${m.getClass}")
    }
  }

  private def fetchMessageBody(message: BytesMessage): Array[Byte] = {
    val buffer = new Array[Byte](message.getBodyLength.toInt)
    message.readBytes(buffer)
    buffer
  }
}
