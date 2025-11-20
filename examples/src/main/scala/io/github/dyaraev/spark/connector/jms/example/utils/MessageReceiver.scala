package io.github.dyaraev.spark.connector.jms.example.utils

import com.typesafe.scalalogging.Logger
import io.github.dyaraev.spark.connector.jms.common.client.JmsSourceClient
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqBroker.ActiveMqAddress
import jakarta.jms.{BytesMessage, TextMessage}
import org.apache.activemq.ActiveMQConnectionFactory

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

object MessageReceiver {

  private val logger = Logger(getClass)

  private val WaitTimeout = 30.seconds

  def withMessageReceiver(amqAddress: ActiveMqAddress, queue: String, receiveTimeout: Duration, logEmpty: Boolean)(
      f: () => Try[Unit]
  ): Try[Unit] = {
    val stopRef = new AtomicBoolean(false)
    newReceiver(amqAddress, queue).flatMap { receiver =>
      val future = receiver.run(receiveTimeout, logEmpty, stopRef)
      val result = Try(f().get)

      stopRef.set(true)
      Try[Unit](Await.ready(future, WaitTimeout))
        .recover { case e => logger.warn("Message receiver execution error", e) }

      result
    }
  }

  private def newReceiver(amqAddress: ActiveMqAddress, queue: String): Try[MessageReceiver] = {
    createSource(amqAddress, queue).map(source => new MessageReceiver(source))
  }

  private def createSource(address: ActiveMqAddress, queue: String): Try[JmsSourceClient] = Try {
    val factory = new ActiveMQConnectionFactory(address.toString)
    JmsSourceClient(factory, queue, transacted = false)
  }
}

private class MessageReceiver(source: JmsSourceClient) {

  import MessageReceiver.logger

  private def run(receiveTimeout: Duration, logEmpty: Boolean, stopRef: AtomicBoolean): Future[Unit] = Future {
    logger.info("Starting message receiver ...")
    while (!stopRef.get()) {
      receiveAndPrint(receiveTimeout, logEmpty)
        .recover { case e => logger.error(s"Failed to receive message", e) }
    }
    source.closeSilently()
    logger.info("Message receiver stopped")
  }

  private def receiveAndPrint(receiveTimeout: Duration, logEmpty: Boolean): Try[Unit] = Try {
    Option(source.receive(receiveTimeout.toMillis)) match {
      case Some(m: TextMessage)  => logger.info(s"TEXT: ${m.getText}")
      case Some(m: BytesMessage) => logger.info(s"BYTES: ${m.getBodyLength}")
      case Some(m)               => logger.info(s"UNKNOWN: ${m.getClass.getName}")
      case None                  => if (logEmpty) logger.info("No message received")
    }
  }
}
