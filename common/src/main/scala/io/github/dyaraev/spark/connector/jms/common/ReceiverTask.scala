package io.github.dyaraev.spark.connector.jms.common

import io.github.dyaraev.spark.connector.jms.common.client.JmsSourceClient
import jakarta.jms.Message
import org.apache.spark.internal.Logging

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

abstract class ReceiverTask(
    client: JmsSourceClient,
    bufferSize: Int,
    timeoutMs: Option[Long],
    logIntervalMs: Long,
) extends Runnable
    with Logging {

  private val messageBuffer: ListBuffer[Message] = ListBuffer.empty[Message]

  private val receive: () => Message = timeoutMs match {
    case Some(timeout) => () => client.receive(timeout)
    case None          => () => client.receiveNoWait
  }

  protected def updateLog(buffer: ListBuffer[Message]): Unit

  protected def reportException(exception: Throwable): Unit

  override def run(): Unit = {
    logInfo("Starting JMS receiver ...")
    try {
      receiveAndPersist(System.currentTimeMillis())
    } catch {
      case e: Throwable => reportException(e)
    } finally {
      if (client != null) client.closeSilently()
      logInfo("JMS receiver is down")
    }
  }

  @tailrec
  final private def receiveAndPersist(lastBatchTimeMs: Long): Unit = {
    val message = receive()
    if (message != null) messageBuffer += message

    // processing time for `updateLog` should be less than `logIntervalMs`
    val currentTimeMs = System.currentTimeMillis()
    if (isTimeToPersist(lastBatchTimeMs, currentTimeMs) || isBufferFull) {
      if (messageBuffer.nonEmpty) updateLog(messageBuffer)
      receiveAndPersist(currentTimeMs)
    } else {
      receiveAndPersist(lastBatchTimeMs)
    }
  }

  private def isTimeToPersist(lastBatchTimeMs: Long, currentTimeMs: Long): Boolean = {
    currentTimeMs - lastBatchTimeMs >= logIntervalMs
  }

  private def isBufferFull: Boolean = {
    messageBuffer.length >= bufferSize
  }
}
