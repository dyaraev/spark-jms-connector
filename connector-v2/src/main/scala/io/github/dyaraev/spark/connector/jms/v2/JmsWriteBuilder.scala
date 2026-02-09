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
import org.apache.spark.sql.types.{BinaryType, DataType, StringType, StructType}

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

    private val valueType = schema.find(_.name == SourceSchema.FieldValue).map(_.dataType).getOrElse {
      throw new RuntimeException("Missing value type")
    }

    override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
      config.messageFormat match {
        case TextFormat   => new JmsTextWriter(config, schema, valueType)
        case BinaryFormat => new JmsBinaryWriter(config, schema, valueType)
        case format       => throw new RuntimeException(s"Unsupported message format '$format'")
      }
    }
  }

  private class JmsTextWriter(override val config: JmsSinkConfig, schema: StructType, valueType: DataType)
      extends JmsDataWriter[String] {

    private val fromInternalRow = ExpressionEncoder(schema).resolveAndBind().createDeserializer()

    override val fromRow: InternalRow => String = valueType match {
      case StringType => (r: InternalRow) => fromInternalRow(r).getAs[String](SourceSchema.FieldValue)
      case BinaryType => (r: InternalRow) => new String(fromInternalRow(r).getAs[Array[Byte]](SourceSchema.FieldValue))
      case t          => throw new RuntimeException(s"Unsupported value type '$t'")
    }

    override def writeValue(value: String): Unit = withClient(_.sendTextMessage(value))
  }

  private class JmsBinaryWriter(override val config: JmsSinkConfig, schema: StructType, valueType: DataType)
      extends JmsDataWriter[Array[Byte]] {

    private val fromInternalRow = ExpressionEncoder(schema).resolveAndBind().createDeserializer()

    override val fromRow: InternalRow => Array[Byte] = valueType match {
      case StringType => (r: InternalRow) => fromInternalRow(r).getAs[String](SourceSchema.FieldValue).getBytes
      case BinaryType => (r: InternalRow) => fromInternalRow(r).getAs[Array[Byte]](SourceSchema.FieldValue)
      case t          => throw new RuntimeException(s"Unsupported value type '$t'")
    }

    override def writeValue(value: Array[Byte]): Unit = withClient(_.sendBytesMessage(value))
  }
}
