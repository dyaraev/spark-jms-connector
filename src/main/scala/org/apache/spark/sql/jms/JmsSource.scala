package org.apache.spark.sql.jms

import io.github.dyaraev.spark.connector.jms.common.client.JmsSourceClient
import io.github.dyaraev.spark.connector.jms.common.config.JmsSourceConfig
import io.github.dyaraev.spark.connector.jms.common.metadata.{LogEntry, MetadataLog}
import io.github.dyaraev.spark.connector.jms.common.{ConnectionFactoryProvider, ReceiverTask}
import jakarta.jms.Message
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset, Source}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}

import javax.annotation.concurrent.GuardedBy
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class JmsSource[T <: LogEntry: ClassTag](
    override val schema: StructType,
    sqlContext: SQLContext,
    config: JmsSourceConfig,
    metadataPath: String,
)(implicit toEntry: Message => T)
    extends Source
    with Logging {

  @GuardedBy("this")
  private var receiverException: Throwable = _

  @GuardedBy("this")
  private val metadataLog: MetadataLog[T] = new MetadataLog[T](sqlContext.sparkSession, metadataPath)

  @GuardedBy("this")
  private var currentOffset: Option[LongOffset] = metadataLog.getLatestBatchId().map(LongOffset(_))

  initialize()

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
      sqlContext.sparkContext.makeRDD(logData.map(_.toInternalRow))
    }
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def stop(): Unit = logInfo("Stopping JMS source ...")

  private def deserializeOffset(offset: Offset): LongOffset = offset match {
    case o: LongOffset       => o
    case o: SerializedOffset => LongOffset(o.json.toLong)
  }

  private def initialize(): Unit = {
    val provider = ConnectionFactoryProvider.createInstance(config.connection.factoryProvider)
    val client: JmsSourceClient = JmsSourceClient(provider, config.connection)
    val receiverTask = new ReceiverTask(client, config.bufferSize, config.timeoutMs, config.logIntervalMs) {

      override protected def updateLog(buffer: ListBuffer[Message]): Unit = JmsSource.this.synchronized {
        currentOffset = currentOffset.map(_ + 1).orElse(Some(LongOffset(0L)))
        logDebug(s"Updated current offset to $currentOffset")

        logInfo(s"Writing data to the WAL for offset $currentOffset ...")
        val records = buffer.map(LogEntry.fromMessage[T])
        metadataLog.add(currentOffset.map(_.offset).getOrElse(0), records.toArray)
        logInfo(s"Updated the WAL for offset $currentOffset")

        logInfo(s"Acknowledging ${buffer.length} messages ...")
        buffer.last.acknowledge()
        buffer.clear()
      }

      override protected def reportException(exception: Throwable): Unit = JmsSource.this.synchronized {
        if (receiverException == null) receiverException = exception
      }
    }

    val receiverThread = new Thread(receiverTask, s"JmsReceiver[${provider.brokerName},${config.connection.queueName}]")
    receiverThread.setDaemon(true)
    receiverThread.start()
  }
}
