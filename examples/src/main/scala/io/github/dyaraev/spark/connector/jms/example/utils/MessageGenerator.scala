package io.github.dyaraev.spark.connector.jms.example.utils

import com.typesafe.scalalogging.Logger
import io.github.dyaraev.spark.connector.jms.common.client.JmsSinkClient
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqBroker.ActiveMqAddress
import org.apache.activemq.ActiveMQConnectionFactory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

object MessageGenerator {

  private val logger = Logger(getClass)

  private val WaitTimeout = 30.seconds

  def withGenerator(amqAddress: ActiveMqAddress, queue: String, sendInterval: Duration, logInterval: Duration)(
      f: () => Try[Unit]
  ): Try[Unit] = {
    val stopRef = new AtomicBoolean(false)
    val counterRef = new AtomicInteger(0)
    newGenerator(amqAddress, queue).flatMap { generator =>
      val futures = Seq(
        generator.send(sendInterval, counterRef, stopRef),
        generator.log(logInterval, counterRef, stopRef),
      )

      val result = Try(f().get)

      stopRef.set(true)
      Try[Unit](Await.ready(Future.sequence(futures), WaitTimeout))
        .recover { case e => logger.warn("Message receiver execution error", e) }

      result
    }
  }

  private def newGenerator(amqAddress: ActiveMqAddress, queue: String): Try[MessageGenerator] = {
    createSink(amqAddress, queue).map(source => new MessageGenerator(source))
  }

  private def createSink(address: ActiveMqAddress, queue: String): Try[JmsSinkClient] = Try {
    val factory = new ActiveMQConnectionFactory(address.toString)
    JmsSinkClient(factory, queue, transacted = false)
  }
}

private class MessageGenerator(sink: JmsSinkClient) {

  import MessageGenerator.logger

  private def send(interval: Duration, counterRef: AtomicInteger, stopRef: AtomicBoolean): Future[Unit] = Future {
    logger.info("Starting message generator ...")
    while (!stopRef.get()) {
      Try(sink.sendTextMessage(counterRef.get.toString))
        .recover { case e => logger.error(s"Failed to receive message", e) }
      Try(Thread.sleep(interval.toMillis))
        .recover { case e => logger.error("Error while sleeping", e) }
      counterRef.incrementAndGet()
    }
    sink.closeSilently()
    logger.info("Message generator stopped")
  }

  private def log(interval: Duration, counterRef: AtomicInteger, stopRef: AtomicBoolean): Future[Unit] = Future {
    while (!stopRef.get()) {
      logger.info(s"Sent messages: ${counterRef.get()}")
      Try(Thread.sleep(interval.toMillis))
        .recover { case e => logger.error("Error while sleeping", e) }
    }
  }
}
