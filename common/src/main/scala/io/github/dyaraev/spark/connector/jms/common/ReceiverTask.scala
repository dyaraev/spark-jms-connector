package io.github.dyaraev.spark.connector.jms.common

import io.github.dyaraev.spark.connector.jms.common.client.JmsSourceClient
import jakarta.jms.Message
import org.apache.spark.internal.Logging

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

abstract class ReceiverTask(
    client: JmsSourceClient,
    bufferSize: Int,
    timeoutMs: Long,
    commitIntervalMs: Long,
) extends Runnable
    with Logging {

  private val messageBuffer: ArrayBuffer[Message] = ArrayBuffer.empty[Message]

  protected def shouldStop(): Boolean = false

  protected def walCommit(messages: Array[Message]): Unit

  protected def reportException(exception: Throwable): Unit

  override def run(): Unit = {
    logInfo("Starting JMS receiver ...")
    try {
      receiveAndPersist(System.currentTimeMillis())
    } catch {
      case NonFatal(e) => reportException(e)
    } finally {
      if (client != null) client.closeSilently()
      logInfo("JMS receiver is down")
    }
  }

  @tailrec
  final private def receiveAndPersist(lastBatchTimeMs: Long): Unit = {
    if (!shouldStop()) {
      val message = client.receive(timeoutMs)
      if (message != null) messageBuffer += message

      // the processing time for `updateLog` should be less than `logIntervalMs`
      val currentTimeMs = System.currentTimeMillis()
      if (isTimeToPersist(lastBatchTimeMs, currentTimeMs) || isBufferFull) {
        if (messageBuffer.nonEmpty) {
          walCommit(messageBuffer.toArray)

          logInfo(s"Committing ${messageBuffer.length} messages ...")
          client.commit()
          messageBuffer.clear()
        }
        receiveAndPersist(currentTimeMs)
      } else {
        receiveAndPersist(lastBatchTimeMs)
      }
    }
  }

  private def isTimeToPersist(lastBatchTimeMs: Long, currentTimeMs: Long): Boolean = {
    currentTimeMs - lastBatchTimeMs >= commitIntervalMs
  }

  private def isBufferFull: Boolean = {
    messageBuffer.length >= bufferSize
  }
}
