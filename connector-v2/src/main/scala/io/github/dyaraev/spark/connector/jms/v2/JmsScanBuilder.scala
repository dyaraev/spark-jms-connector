package io.github.dyaraev.spark.connector.jms.v2

import io.github.dyaraev.spark.connector.jms.common.client.JmsSourceClient
import io.github.dyaraev.spark.connector.jms.common.config.CaseInsensitiveConfigMap.Implicits.CaseInsensitiveStringMapOps
import io.github.dyaraev.spark.connector.jms.common.config.JmsSourceConfig
import io.github.dyaraev.spark.connector.jms.common.config.MessageFormat.{BinaryFormat, TextFormat}
import io.github.dyaraev.spark.connector.jms.common.metadata.LogEntry.Implicits._
import io.github.dyaraev.spark.connector.jms.common.metadata.LogEntry.{BinaryLogEntry, TextLogEntry}
import io.github.dyaraev.spark.connector.jms.common.metadata.{LogEntry, MetadataLog}
import io.github.dyaraev.spark.connector.jms.common.{ReceiverTask, SourceSchema}
import io.github.dyaraev.spark.connector.jms.v2.SparkInternals.SparkLongOffset
import jakarta.jms.{IllegalStateException, Message}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.concurrent.GuardedBy
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class JmsScanBuilder(options: CaseInsensitiveStringMap) extends ScanBuilder {

  private val config = JmsSourceConfig.fromOptions(options.toConfigMap)

  override def build(): Scan = {
    config.messageFormat match {
      case TextFormat   => newTextScan
      case BinaryFormat => newBinaryScan
      case format       => throw new RuntimeException(s"Unsupported message format '$format'")
    }
  }

  private def newTextScan: Scan = new Scan {

    override def readSchema(): StructType = SourceSchema.TextSchema

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      new JmsScanBuilder.JmsMicroBatchStream[TextLogEntry](config, checkpointLocation)
    }
  }

  private def newBinaryScan: Scan = new Scan {

    override def readSchema(): StructType = SourceSchema.BinarySchema

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      new JmsScanBuilder.JmsMicroBatchStream[BinaryLogEntry](config, checkpointLocation)
    }
  }
}

object JmsScanBuilder {

  private class JmsMicroBatchStream[T <: LogEntry: ClassTag](config: JmsSourceConfig, checkpointLocation: String)(
      implicit toEntry: Message => T
  ) extends MicroBatchStream
      with Logging {

    private val identifier: String = s"${config.connection.provider}:${config.connection.queue}"

    private val spark = SparkSession.active

    @GuardedBy("this")
    private var receiverException: Throwable = _

    @GuardedBy("this")
    private val metadataLog: MetadataLog[T] = new MetadataLog[T](spark, checkpointLocation)

    @GuardedBy("this")
    private var currentOffset: SparkLongOffset = SparkLongOffset(metadataLog.getLatestBatchId().getOrElse(-1L))

    @GuardedBy("this")
    private var previousOffset: SparkLongOffset = SparkLongOffset(-1L)

    @GuardedBy("this")
    private var stopFlag: Boolean = false

    private val initialized: AtomicBoolean = new AtomicBoolean(false)

    private val numPartitions: Int = config.numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    override def initialOffset(): Offset = synchronized {
      logInfo("Resetting offset")
      currentOffset = SparkLongOffset(-1L)
      currentOffset
    }

    override def latestOffset(): Offset = synchronized {
      if (initialized.compareAndSet(false, true)) initialize()
      if (receiverException != null) throw new RuntimeException("JMS receiver error", receiverException)

      logInfo(s"Retrieved latest offset $currentOffset")
      currentOffset
    }

    // TODO: avoid materializing data on the driver
    override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
      val startOrdinal = start.asInstanceOf[SparkLongOffset].offset + 1
      val endOrdinal = end.asInstanceOf[SparkLongOffset].offset

      val rawList = synchronized {
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

    override def commit(end: Offset): Unit = synchronized {
      end match {
        case currentOffset: SparkLongOffset =>
          val diff = currentOffset.offset - previousOffset.offset
          if (diff < 0) throw new IllegalStateException(s"Offsets out of order [$previousOffset, $currentOffset]")
          metadataLog.purge(currentOffset.offset - config.numOffsetsToKeep)
          previousOffset = currentOffset
      }
    }

    override def stop(): Unit = synchronized {
      logInfo("Stopping JMS source ...")
      stopFlag = true
    }

    override def deserializeOffset(json: String): Offset = SparkLongOffset(json.toLong)

    override def toString: String = s"JmsSourceV2[$identifier]"

    private def initialize(): Unit = synchronized {
      val client = JmsSourceClient(config.connection, transacted = true)
      val receiverTask = new ReceiverTask(client, config.bufferSize, config.receiveTimeoutMs, config.commitIntervalMs) {

        override protected def shouldStop(): Boolean = JmsMicroBatchStream.this.synchronized {
          JmsMicroBatchStream.this.stopFlag
        }

        override protected def walCommit(messages: Array[Message]): Unit = JmsMicroBatchStream.this.synchronized {
          currentOffset += 1

          logInfo(s"Writing data to the WAL for offset $currentOffset ...")
          val records = messages.map(LogEntry.fromMessage[T])
          metadataLog.add(currentOffset.offset, records)
          logInfo(s"Updated the WAL for offset $currentOffset")
        }

        override protected def reportException(exception: Throwable): Unit = JmsMicroBatchStream.this.synchronized {
          if (receiverException == null) receiverException = exception
        }
      }

      val receiverThread = new Thread(receiverTask, s"JmsReceiver[$identifier]")
      receiverThread.setDaemon(true)
      receiverThread.start()
    }
  }

  private case class JmsInputPartition(slice: ArrayBuffer[InternalRow]) extends InputPartition
}
