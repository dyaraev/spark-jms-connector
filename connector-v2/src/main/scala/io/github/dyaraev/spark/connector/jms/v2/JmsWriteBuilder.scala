package io.github.dyaraev.spark.connector.jms.v2

import io.github.dyaraev.spark.connector.jms.common.SourceSchema
import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits._
import io.github.dyaraev.spark.connector.jms.common.config.JmsSinkConfig
import io.github.dyaraev.spark.connector.jms.common.config.MessageFormat.{BinaryFormat, TextFormat}
import io.github.dyaraev.spark.connector.jms.v2.JmsDataWriter.JmsCommitMessage
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}

class JmsWriteBuilder(info: LogicalWriteInfo) extends WriteBuilder {

  override def build(): Write = new Write {

    private val schema: StructType = info.schema()
    private val config: JmsSinkConfig = JmsSinkConfig.fromOptions(info.options().toConfigMap)

    override def toStreaming: StreamingWrite = new StreamingWrite with Logging {

      override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
        new JmsWriteBuilder.JmsStreamingDataWriterFactory(schema, config)
      }

      override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
        val totalMessages = messages.foldLeft(0) { case (acc, JmsCommitMessage(n)) => acc + n }
        logInfo(s"Send $totalMessages messages")
      }

      override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
        logWarning("Aborting ...")
      }
    }
  }
}

object JmsWriteBuilder {

  private class JmsStreamingDataWriterFactory(schema: StructType, config: JmsSinkConfig)
      extends StreamingDataWriterFactory {

    private val fromInternalRow = ExpressionEncoder(schema).resolveAndBind().createDeserializer()

    private val valueFieldType = schema.find(_.name == SourceSchema.FieldValue).map(_.dataType) match {
      case Some(t) => t
      case None    => throw new RuntimeException("Missing value type")
    }

    override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
      config.messageFormat match {
        case TextFormat   => newStringWriter
        case BinaryFormat => newBinaryWriter
        case format       => throw new RuntimeException(s"Unsupported message format '$format'")
      }
    }

    private def newStringWriter: DataWriter[InternalRow] = new JmsDataWriter[String] {

      override val config: JmsSinkConfig = JmsStreamingDataWriterFactory.this.config

      override val fromRow: InternalRow => String = valueFieldType match {
        case StringType =>
          (r: InternalRow) => fromInternalRow(r).getAs[String](SourceSchema.FieldValue)
        case BinaryType =>
          (r: InternalRow) => new String(fromInternalRow(r).getAs[Array[Byte]](SourceSchema.FieldValue))
        case t =>
          throw new RuntimeException(s"Unsupported value type '$t'")
      }

      override def writeValue(value: String): Unit = sendMessage(_.sendTextMessage(value))
    }

    private def newBinaryWriter: DataWriter[InternalRow] = new JmsDataWriter[Array[Byte]] {

      override val config: JmsSinkConfig = JmsStreamingDataWriterFactory.this.config

      override val fromRow: InternalRow => Array[Byte] = valueFieldType match {
        case StringType =>
          (r: InternalRow) => fromInternalRow(r).getAs[String](SourceSchema.FieldValue).getBytes
        case BinaryType =>
          (r: InternalRow) => fromInternalRow(r).getAs[Array[Byte]](SourceSchema.FieldValue)
        case t =>
          throw new RuntimeException(s"Unsupported value type '$t'")
      }

      override def writeValue(value: Array[Byte]): Unit = sendMessage(_.sendBytesMessage(value))
    }
  }
}
