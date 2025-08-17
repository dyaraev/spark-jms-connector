package io.github.dyaraev.spark.connector.jms

import io.github.dyaraev.spark.connector.jms.JmsMicroBatchStream.{JmsInputPartition, LogEntry}
import jakarta.jms._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, LongOffset}
import org.apache.spark.unsafe.types.UTF8String

import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.concurrent.GuardedBy
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class JmsMicroBatchStream(
    config: JmsSourceConfig,
    numPartitions: Int,
    checkpointLocation: String,
    jmsConnector: JmsConnector,
) extends MicroBatchStream
    with Logging {

  @GuardedBy("this")
  private var connection: Connection = _

  @GuardedBy("this")
  private var receiverThread: Thread = _

  @GuardedBy("this")
  private var receiverException: Throwable = _

  @GuardedBy("this")
  private val metadataLog: HDFSMetadataLog[Array[LogEntry]] = {
    new HDFSMetadataLog(SparkSession.active, checkpointLocation)
  }

  @GuardedBy("this")
  private var currentOffset: LongOffset = this.synchronized {
    LongOffset(metadataLog.getLatest().map(_._1).getOrElse(-1L))
  }

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  @volatile
  private var isStopping: Boolean = false

  private def initialize(): Unit = synchronized {
    logInfo("Starting JMS receiver")
    connection = createConnection
    connection.start()

    receiverThread = new Thread(s"JmsSource(${jmsConnector.brokerName}, ${config.queueName})") {
      setDaemon(true)

      private var lastBatchTimeMs = System.currentTimeMillis()
      private val messageBuffer = ListBuffer.empty[Message]

      override def run(): Unit = {
        try {
          val consumer = createConsumer(connection)

          val rc = config.receiver
          while (!isStopping) {
            val message = rc.timeoutMs.map(consumer.receive).getOrElse(consumer.receiveNoWait())
            if (message != null) messageBuffer += message

            val currentTimeMs = System.currentTimeMillis()
            if (currentTimeMs - lastBatchTimeMs >= rc.intervalMs || messageBuffer.length >= rc.bufferSize) {
              if (messageBuffer.nonEmpty) {
                JmsMicroBatchStream.this.synchronized {
                  currentOffset += 1

                  logInfo(s"Writing data to the WAL for offset $currentOffset")
                  val records = messageBuffer.map(LogEntry.fromMessage)
                  metadataLog.add(currentOffset.offset, records.toArray)
                  logInfo(s"Updated the WAL for offset $currentOffset")

                  logInfo(s"Acknowledging ${messageBuffer.length} messages ...")
                  messageBuffer.last.acknowledge()
                  messageBuffer.clear()
                }
              }
              lastBatchTimeMs = currentTimeMs
            }
          }
        } catch {
          case e: Throwable => if (receiverException == null) receiverException = e
        } finally {
          logWarning("JMS receiver is down")
        }
      }
    }
    receiverThread.start()
  }

  override def initialOffset(): Offset = this.synchronized {
    logInfo("Resetting offset")
    currentOffset = LongOffset(-1L)
    currentOffset
  }

  override def latestOffset(): Offset = this.synchronized {
    if (initialized.compareAndSet(false, true)) initialize()
    if (receiverException != null) throw new RuntimeException("JMS receiver error", receiverException)

    logInfo(s"Retrieved latest offset $currentOffset")
    currentOffset
  }

  override def deserializeOffset(json: String): Offset = LongOffset(json.toLong)

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOrdinal = start.asInstanceOf[LongOffset].offset + 1
    val endOrdinal = end.asInstanceOf[LongOffset].offset

    val rawList = this.synchronized {
      if (endOrdinal - startOrdinal >= 0) {
        metadataLog.get(Some(startOrdinal), Some(endOrdinal)).flatMap(_._2)
      } else {
        Array.empty[LogEntry]
      }
    }

    val slices = Array.fill(numPartitions)(new ArrayBuffer[InternalRow])
    rawList.map(_.toInternalRow).zipWithIndex.foreach { case (r, idx) =>
      slices(idx % numPartitions).append(r)
    }

    slices.map(JmsInputPartition)
  }

  override def createReaderFactory(): PartitionReaderFactory = { (partition: InputPartition) =>
    {
      val slice = partition.asInstanceOf[JmsInputPartition].slice
      new PartitionReader[InternalRow] {
        private var currentIdx = -1

        override def next(): Boolean = {
          currentIdx += 1
          currentIdx < slice.size
        }

        override def get(): InternalRow = slice(currentIdx)

        override def close(): Unit = {}
      }
    }
  }

  override def commit(end: Offset): Unit = synchronized {
    val newOffset = end.asInstanceOf[LongOffset]
    logInfo(s"Committing offset $newOffset")

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt
    if (offsetDiff < 0) {
      throw new IllegalStateException(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    lastOffsetCommitted = newOffset
    metadataLog.purge(lastOffsetCommitted.offset - 7) // keeping last 10 offsets
  }

  override def stop(): Unit = synchronized {
    logInfo("Stopping JMS receiver ...")
    isStopping = true
    if (connection != null) {
      try connection.stop()
      catch { case e: Throwable => logError("Unexpected error while stopping the connection", e) }

      try connection.close()
      catch { case e: Throwable => logError("Unexpected error while closing the connection", e) }
    }
  }

  override def toString: String = s"JmsV2[type: ${jmsConnector.brokerName}, queue: ${config.queueName}]"

  private def createConnection: Connection = {
    val connection = jmsConnector.getConnectionFactory(config.brokerOptions).createConnection()
    connection.setExceptionListener { e: JMSException => logError(s"Connection error in JMS receiver", e) }
    connection
  }

  private def createConsumer(connection: Connection): MessageConsumer = {
    val jmsSession = connection.createSession(false, JMSContext.CLIENT_ACKNOWLEDGE)
    val jmsQueue = jmsSession.createQueue(config.queueName)
    jmsSession.createConsumer(jmsQueue, config.messageSelector.orNull)
  }
}

object JmsMicroBatchStream extends Logging {

  private val Base64Decoder = java.util.Base64.getMimeDecoder
  private val Base64Encoder = java.util.Base64.getMimeEncoder

  case class JmsInputPartition(slice: ArrayBuffer[InternalRow]) extends InputPartition

  case class LogEntry(id: String, sentTs: Long, receivedTs: Long, value: String) {

    def toInternalRow: InternalRow = {
      InternalRow(UTF8String.fromString(id), sentTs, receivedTs, Base64Decoder.decode(value))
    }
  }

  object LogEntry {

    def fromMessage(message: Message): LogEntry = {
      val currentTimeMs = System.currentTimeMillis()
      message match {
        case tm: TextMessage =>
          val encodedValue = Base64Encoder.encodeToString(tm.getText.getBytes)
          LogEntry(tm.getJMSMessageID, tm.getJMSTimestamp, currentTimeMs, encodedValue)
        case bm: BytesMessage =>
          val buffer = new Array[Byte](bm.getBodyLength.toInt)
          bm.readBytes(buffer)
          val encodedValue = Base64Encoder.encodeToString(buffer)
          LogEntry(bm.getJMSMessageID, bm.getJMSTimestamp, currentTimeMs, encodedValue)
        case m =>
          throw new RuntimeException(s"Unsupported message implementation ${m.getClass}")
      }
    }
  }
}
