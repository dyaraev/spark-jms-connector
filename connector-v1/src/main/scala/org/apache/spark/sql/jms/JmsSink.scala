package org.apache.spark.sql.jms

import io.github.dyaraev.spark.connector.jms.common.SourceSchema
import io.github.dyaraev.spark.connector.jms.common.client.JmsSinkClient
import io.github.dyaraev.spark.connector.jms.common.config.JmsSinkConfig
import io.github.dyaraev.spark.connector.jms.common.config.MessageFormat.{BinaryFormat, TextFormat}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression, UnsafeProjection}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.jms.JmsSink.{MaxSendAttempts, MinRetryInterval}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

import scala.annotation.tailrec

class JmsSink(config: JmsSinkConfig) extends Sink with Serializable with Logging {

  @transient
  private var client: JmsSinkClient = _

  @volatile
  private var latestBatchId = -1L

  override def toString: String = "JmsSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logWarning(s"Skipping already committed batch $batchId")
    } else {
      val queryExecution = data.queryExecution
      config.messageFormat match {
        case TextFormat   => sendTextMessages(queryExecution)
        case BinaryFormat => sendBytesMessages(queryExecution)
        case another      => throw new RuntimeException(s"Unsupported message format: $another")
      }
      latestBatchId = batchId
    }
  }

  private def sendBytesMessages(queryExecution: QueryExecution): Unit = {
    val schema = queryExecution.analyzed.output
    queryExecution.toRdd.foreachPartition { iter =>
      val expressions = Seq(Cast(valueExpression(schema, Seq(BinaryType, StringType)), BinaryType))
      val projection = UnsafeProjection.create(expressions, schema)
      iter.foreach(ir => sendMessage(_.sendBytesMessage(projection(ir).getBinary(0))))
    }
  }

  private def sendTextMessages(queryExecution: QueryExecution): Unit = {
    val schema = queryExecution.analyzed.output
    queryExecution.toRdd.foreachPartition { iter =>
      val expressions = Seq(Cast(valueExpression(schema, Seq(BinaryType, StringType)), StringType))
      val projection = UnsafeProjection.create(expressions, schema)
      iter.foreach(ir => sendMessage(_.sendTextMessage(projection(ir).getString(0))))
    }
  }

  private def valueExpression(schema: Seq[Attribute], dataTypes: Seq[DataType]): Expression = {
    schema.find(_.name == SourceSchema.FieldValue) match {
      case Some(expr) if dataTypes.exists(_.sameType(expr.dataType)) => expr
      case Some(expr) => throw new RuntimeException(s"Wrong value data type ${expr.dataType}")
      case None       => throw new RuntimeException("Missing field value")
    }
  }

  @tailrec
  private def sendMessage(f: JmsSinkClient => Unit, attempt: Int = 1): Unit = {
    try f(getOrCreateWriter())
    catch {
      case e: Throwable =>
        if (attempt <= MaxSendAttempts) {
          logError(s"Error sending message (attempt=$attempt), retrying ...", e)
          Thread.sleep(attempt * MinRetryInterval)
          closeClientIfExists()
          sendMessage(f, attempt + 1)
        } else {
          throw new RuntimeException("Unable to send message", e)
        }
    }
  }

  private def getOrCreateWriter(): JmsSinkClient = {
    if (client == null) {
      client = JmsSinkClient(config.connection, None)
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

object JmsSink {

  private val MaxSendAttempts: Int = 3
  private val MinRetryInterval: Long = 5000
}
