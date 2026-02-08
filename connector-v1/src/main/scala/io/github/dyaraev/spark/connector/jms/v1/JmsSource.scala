package io.github.dyaraev.spark.connector.jms.v1

import io.github.dyaraev.spark.connector.jms.common.ReceiverTask
import io.github.dyaraev.spark.connector.jms.common.client.JmsSourceClient
import io.github.dyaraev.spark.connector.jms.common.config.JmsSourceConfig
import io.github.dyaraev.spark.connector.jms.common.metadata.{LogEntry, MetadataLog}
import jakarta.jms.Message
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.jms.SparkInternals
import org.apache.spark.sql.jms.SparkInternals.{SparkLongOffset, SparkSerializedOffset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}

import javax.annotation.concurrent.GuardedBy
import scala.reflect.ClassTag

class JmsSource[T <: LogEntry: ClassTag](
    override val schema: StructType,
    sqlContext: SQLContext,
    config: JmsSourceConfig,
    metadataPath: String,
)(implicit toEntry: Message => T)
    extends Source
    with Logging {

  private val identifier: String = s"${config.connection.brokerName}:${config.connection.queueName}"

  @GuardedBy("this")
  private var receiverException: Throwable = _

  @GuardedBy("this")
  private val metadataLog: MetadataLog[T] = new MetadataLog[T](sqlContext.sparkSession, metadataPath)

  @GuardedBy("this")
  private var currentOffset: Option[SparkLongOffset] = metadataLog.getLatestBatchId().map(SparkLongOffset(_))

  @GuardedBy("this")
  private var stopFlag = false

  initialize()

  override def toString: String = "JmsSourceV1"

  override def getOffset: Option[Offset] = synchronized {
    if (receiverException != null) throw new RuntimeException("JMS receiver error", receiverException)

    logInfo(s"Retrieved latest offset $currentOffset")
    currentOffset.foreach(offset => metadataLog.purge(offset.offset - config.numOffsetsToKeep))
    currentOffset
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = synchronized {
    val rdd = if (start.contains(end)) {
      sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty")
    } else {
      val startOffset = start.map(deserializeOffset(_).offset).map(_ + 1)
      val endOrdinal = deserializeOffset(end).offset

      val logData = metadataLog.get(startOffset, Some(endOrdinal)).flatMap(_._2)
      sqlContext.sparkContext.makeRDD(logData.map(_.toInternalRow).toIndexedSeq)
    }
    SparkInternals.createDataFrame(sqlContext, rdd, schema, isStreaming = true)
  }

  override def stop(): Unit = synchronized {
    logInfo("Stopping JMS source ...")
    stopFlag = true
  }

  private def deserializeOffset(offset: Offset): SparkLongOffset = offset match {
    case o: SparkLongOffset       => o
    case o: SparkSerializedOffset => SparkLongOffset(o.json.toLong)
  }

  private def initialize(): Unit = {
    val client: JmsSourceClient = JmsSourceClient(config.connection, transacted = true)
    val receiverTask = new ReceiverTask(client, config.bufferSize, config.receiveTimeoutMs, config.commitIntervalMs) {

      override protected def shouldStop(): Boolean = JmsSource.this.synchronized {
        JmsSource.this.stopFlag
      }

      override protected def walCommit(messages: Array[Message]): Unit = JmsSource.this.synchronized {
        currentOffset = currentOffset.map(_ + 1).orElse(Some(SparkLongOffset(0L)))
        logDebug(s"Updated current offset to $currentOffset")

        currentOffset.foreach(offset => logInfo(s"Writing data to the WAL for offset $offset ..."))
        val records = messages.map(LogEntry.fromMessage[T])
        metadataLog.add(currentOffset.map(_.offset).getOrElse(0), records)
        currentOffset.foreach(offset => logInfo(s"Updated the WAL for offset $offset"))
      }

      override protected def reportException(exception: Throwable): Unit = JmsSource.this.synchronized {
        if (receiverException == null) receiverException = exception
      }
    }

    val receiverThread = new Thread(receiverTask, s"JmsReceiver[$identifier]")
    receiverThread.setDaemon(true)
    receiverThread.start()
  }
}
