package io.github.dyaraev.spark.connector.jms.v1

import io.github.dyaraev.spark.connector.jms.common.SourceSchema
import io.github.dyaraev.spark.connector.jms.common.client.JmsSinkClient
import io.github.dyaraev.spark.connector.jms.common.config.JmsSinkConfig
import io.github.dyaraev.spark.connector.jms.common.config.MessageFormat.{BinaryFormat, TextFormat}
import io.github.dyaraev.spark.connector.jms.common.utils.CommonUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.jms.SparkInternals
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

import scala.util.control.NonFatal

class JmsSink(config: JmsSinkConfig) extends Sink with Serializable with Logging {

  @volatile
  private var latestBatchId = -1L

  override def toString: String = "JmsSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logWarning(s"Skipping already committed batch $batchId")
    } else {
      val queryExecution = data.queryExecution
      val schema = queryExecution.analyzed.output
      val rdd = queryExecution.toRdd
      config.messageFormat match {
        case TextFormat   => sendTextMessages(rdd, schema, batchId)
        case BinaryFormat => sendBytesMessages(rdd, schema, batchId)
        case another      => throw new RuntimeException(s"Unsupported message format: $another")
      }
      latestBatchId = batchId
    }
  }

  private def sendBytesMessages(rdd: RDD[InternalRow], schema: Seq[Attribute], batchId: Long): Unit = {
    rdd.foreachPartition { iter =>
      new JmsSink.BytesMessageSender(config, schema).sendAll(iter, batchId)
    }
  }

  private def sendTextMessages(rdd: RDD[InternalRow], schema: Seq[Attribute], batchId: Long): Unit = {
    rdd.foreachPartition { iter =>
      new JmsSink.TextMessageSender(config, schema).sendAll(iter, batchId)
    }
  }
}

object JmsSink {

  private trait MessageSender[T] extends Logging {

    protected def config: JmsSinkConfig

    protected def schema: Seq[Attribute]

    protected def expressions: Seq[Expression]

    protected def fromRow(row: UnsafeRow): T

    protected def send(client: JmsSinkClient, message: T): Unit

    def sendAll(iter: Iterator[InternalRow], batchId: Long): Unit = {
      val projection = UnsafeProjection.create(expressions, schema)
      var counter = 0
      withClient(batchId) { client =>
        iter.foreach { row =>
          val unsafeRow = projection(row)
          if (unsafeRow.isNullAt(0)) {
            throw new RuntimeException("Field 'value' contains null")
          } else {
            val message = fromRow(unsafeRow)
            send(client, message)
            counter += 1
            config.throttlingDelayMs.foreach(Thread.sleep)
          }
        }
        commitJmsTransaction(client, counter)
      }
    }

    protected def valueExpression(schema: Seq[Attribute], dataTypes: Seq[DataType]): Expression = {
      schema.find(_.name == SourceSchema.FieldValue) match {
        case Some(expr) if dataTypes.exists(dt => SparkInternals.sameTypes(dt, expr.dataType)) => expr
        case Some(expr) => throw new RuntimeException(s"Wrong value data type ${expr.dataType}")
        case None       => throw new RuntimeException("Missing field value")
      }
    }

    private def withClient(batchId: Long)(f: JmsSinkClient => Unit): Unit = {
      var client: JmsSinkClient = null
      try {
        client = JmsSinkClient(config.connection, transacted = true)
        f(client)
      } catch {
        case NonFatal(e) =>
          if (client != null) rollbackJmsTransaction(client)
          throw new RuntimeException(CommonUtils.formatMessage("Failed to send JMS message(s)", Some(batchId)), e)
      } finally {
        if (client != null) client.closeSilently()
      }
    }

    private def commitJmsTransaction(client: JmsSinkClient, numMessages: Int): Unit = {
      if (numMessages > 0) {
        logInfo(s"Committing $numMessages JMS messages")
        try client.commit()
        catch {
          case NonFatal(e) =>
            throw new RuntimeException("JMS commit failed", e)
        }
      }
    }

    private def rollbackJmsTransaction(client: JmsSinkClient): Unit = {
      try client.rollback()
      catch {
        case e: Throwable =>
          logError("Unable to rollback transaction", e)
      }
    }
  }

  private class TextMessageSender(override val config: JmsSinkConfig, override val schema: Seq[Attribute])
      extends MessageSender[String] {

    override protected def expressions: Seq[Expression] =
      Seq(Cast(valueExpression(schema, Seq(BinaryType, StringType)), StringType))

    override protected def fromRow(row: UnsafeRow): String =
      row.getString(0)

    override protected def send(client: JmsSinkClient, message: String): Unit =
      client.sendTextMessage(message)
  }

  private class BytesMessageSender(override val config: JmsSinkConfig, override val schema: Seq[Attribute])
      extends MessageSender[Array[Byte]] {

    override protected def expressions: Seq[Expression] =
      Seq(Cast(valueExpression(schema, Seq(BinaryType, StringType)), BinaryType))

    override protected def fromRow(row: UnsafeRow): Array[Byte] =
      row.getBinary(0)

    override protected def send(client: JmsSinkClient, message: Array[Byte]): Unit =
      client.sendBytesMessage(message)
  }
}
