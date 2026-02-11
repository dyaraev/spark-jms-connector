package io.github.dyaraev.spark.connector.jms.example.utils

import com.typesafe.scalalogging.Logger
import io.github.dyaraev.spark.connector.jms.common.client.JmsSinkClient
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqBroker.ActiveMqAddress
import org.apache.activemq.ActiveMQConnectionFactory

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

object MessageGenerator {

  private val logger = Logger(getClass)

  private val ShutdownTimeoutSec = 10L

  def withGenerator(
      fields: List[CsvValueGenerator],
      amqAddress: ActiveMqAddress,
      queue: String,
      sendInterval: Duration, // should be greater than 1 second
      logInterval: Duration, // should be greater than 1 second
  )(f: () => Try[Unit]): Try[Unit] = {
    createSink(amqAddress, queue).flatMap { source =>
      val recordGenerator = new CsvRecordGenerator(fields)
      val messageGenerator = new MessageGenerator(recordGenerator, source)
      val result = messageGenerator.start(sendInterval, logInterval).flatMap(_ => Try(f()).flatten)
      messageGenerator.stop().recover { case NonFatal(e) => logger.warn("Error while stopping message generator", e) }
      source.closeSilently()
      result
    }
  }

  private def createSink(address: ActiveMqAddress, queue: String): Try[JmsSinkClient] = Try {
    val factory = new ActiveMQConnectionFactory(address.toString)
    JmsSinkClient(factory, queue, transacted = false)
  }
}

private class MessageGenerator(recordGenerator: CsvRecordGenerator, sink: JmsSinkClient) {

  import MessageGenerator.logger

  // Using AtomicInteger to make sure the code is valid for multi-thread execution
  private val counter = new AtomicInteger(0)

  // For simplicity, a single thread is used, as the code is only intended for testing.
  private val executor = Executors.newSingleThreadScheduledExecutor()

  private def start(sendInterval: Duration, logInterval: Duration): Try[Unit] = Try {
    logger.info(s"Starting message generator ($sendInterval, $logInterval) ...")
    executor.scheduleAtFixedRate(() => send(), 0L, sendInterval.toMillis, TimeUnit.MILLISECONDS)
    executor.scheduleAtFixedRate(() => log(), 0L, logInterval.toMillis, TimeUnit.MILLISECONDS)
    logger.info("Message generator started")
  }

  private def stop(): Try[Unit] = Try {
    logger.info("Stopping message generator ...")
    executor.shutdown()
    if (!executor.awaitTermination(MessageGenerator.ShutdownTimeoutSec, TimeUnit.SECONDS)) {
      executor.shutdownNow()
    }
    logger.info("Message generator stopped")
  }

  private def send(): Unit = synchronized {
    recordGenerator
      .generateRow(counter.get())
      .flatMap(csvLine => sendMessage(csvLine))
      .map[Unit](_ => counter.incrementAndGet())
      .recover { case NonFatal(e) => logger.error(s"Failed to send message", e) }
      .get
  }

  private def log(): Unit = synchronized {
    logger.info(s"Sent messages: ${counter.get()}")
  }

  private def sendMessage(message: String): Try[Unit] = Try {
    sink.sendTextMessage(message)
  }
}
